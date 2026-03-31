#!/usr/bin/env python3
import argparse
import asyncio
import base64
import csv
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
from collections import deque
from pathlib import Path
from typing import Optional

import requests
import websockets

CHROME_PATH = "C:/Program Files/Google/Chrome/Application/chrome.exe"
FOA_ITAG = "338"
PIPELINE_VERSION = 2


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download YT-Ambigen FOA audio, then export aligned 5-second 4-channel WAV clips."
    )
    parser.add_argument(
        "--csv",
        default="clips.csv",
        help="CSV containing sample ids or file_path values",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="How many sample ids to process",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Start offset into the CSV",
    )
    parser.add_argument(
        "--outdir",
        default="./",
        help="Output directory",
    )
    parser.add_argument(
        "--chrome-path",
        default=CHROME_PATH,
        help="Chrome binary path",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=True,
        help="Run Chrome headless",
    )
    parser.add_argument(
        "--no-headless",
        action="store_false",
        dest="headless",
        help="Run Chrome with a visible window",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Per-step timeout in seconds",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Retries per sample",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress logs",
    )
    parser.add_argument(
        "--clip-seconds",
        type=float,
        default=5.0,
        help="Aligned clip duration to export as WAV",
    )
    parser.add_argument(
        "--download-method",
        choices=["auto", "ytdlp", "sabr"],
        default="auto",
        help="Preferred acquisition method. auto tries yt-dlp first, then SABR fallback.",
    )
    return parser.parse_args()


def load_sample_ids(csv_path: Path):
    with csv_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    ids = []
    for row in rows:
        if "id" in row and row["id"]:
            sample_id = row["id"].strip()
        elif "file_path" in row and row["file_path"]:
            sample_id = Path(row["file_path"].strip()).stem
        else:
            continue
        if sample_id:
            ids.append(sample_id)
    return ids


def split_sample_id(sample_id: str):
    video_id, start_sec = sample_id.rsplit("_", 1)
    return video_id, int(start_sec)


def ensure_dirs(base: Path):
    dirs = {
        "source_webm": base / "source_webm",
        "raw_ump": base / "raw_ump",
        "carved": base / "carved",
        "wav": base / "wav",
        "meta": base / "meta",
        "reports": base / "reports",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def vlog(args, message):
    if args.verbose:
        print(message, flush=True)


def ffprobe_json(path: Path):
    proc = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=index,codec_type,codec_name,channels,channel_layout,duration:format=duration",
            "-of",
            "json",
            str(path),
        ],
        capture_output=True,
        text=True,
    )
    out = proc.stdout.strip()
    if not out:
        return {"streams": [], "format": {}, "stderr": proc.stderr.strip(), "returncode": proc.returncode}
    data = json.loads(out)
    data["stderr"] = proc.stderr.strip()
    data["returncode"] = proc.returncode
    return data


def has_foa_stream(probe):
    for stream in probe.get("streams", []):
        if (
            stream.get("codec_type") == "audio"
            and stream.get("channels") == 4
            and "ambisonic" in (stream.get("channel_layout") or "").lower()
        ):
            return True
    return False


def has_four_channel_audio(probe):
    for stream in probe.get("streams", []):
        if stream.get("codec_type") == "audio" and stream.get("channels") == 4:
            return True
    return False


def has_standard_wav_stream(probe):
    for stream in probe.get("streams", []):
        if (
            stream.get("codec_type") == "audio"
            and stream.get("channels") == 4
            and (stream.get("codec_name") or "").startswith("pcm_")
        ):
            return True
    return False


def has_source_foa_stream(probe):
    return has_foa_stream(probe)


def parse_duration(value):
    try:
        if value in (None, "", "N/A"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def best_probe_duration(probe):
    format_duration = parse_duration((probe.get("format") or {}).get("duration"))
    durations = []
    for stream in probe.get("streams", []):
        duration = parse_duration(stream.get("duration"))
        if duration is not None:
            durations.append(duration)
    if durations:
        return max(durations)
    return format_duration


def trim_text(text: str, limit: int = 4000):
    if not text:
        return text
    if len(text) <= limit:
        return text
    return text[-limit:]


def audio_streams(probe):
    return [stream for stream in probe.get("streams", []) if stream.get("codec_type") == "audio"]


def select_audio_stream_index(probe):
    best_stream = None
    best_key = None
    for stream in audio_streams(probe):
        index = stream.get("index")
        if index is None:
            continue
        key = (
            1 if stream.get("channels") == 4 else 0,
            1 if "ambisonic" in (stream.get("channel_layout") or "").lower() else 0,
            parse_duration(stream.get("duration")) or -1.0,
            -int(index),
        )
        if best_key is None or key > best_key:
            best_stream = stream
            best_key = key
    return None if best_stream is None else int(best_stream["index"])


def parse_opus_head(blob: bytes):
    marker = b"OpusHead"
    pos = blob.find(marker)
    if pos < 0 or pos + 19 > len(blob):
        return None
    head = blob[pos : pos + 19]
    return {
        "offset": pos,
        "version": head[8],
        "channel_count": head[9],
        "pre_skip": int.from_bytes(head[10:12], "little"),
        "input_sample_rate": int.from_bytes(head[12:16], "little"),
        "output_gain": int.from_bytes(head[16:18], "little", signed=False),
        "mapping_family": head[18],
    }


def find_ebml_offsets(data: bytes):
    marker = b"\x1a\x45\xdf\xa3"
    offsets = []
    cursor = 0
    while True:
        pos = data.find(marker, cursor)
        if pos < 0:
            break
        offsets.append(pos)
        cursor = pos + 1
    return offsets


def extract_standard_wav(candidate_path: Path, wav_path: Path):
    proc = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-v",
            "warning",
            "-err_detect",
            "ignore_err",
            "-fflags",
            "+discardcorrupt",
            "-vn",
            "-i",
            str(candidate_path),
            "-c:a",
            "pcm_s16le",
            str(wav_path),
        ],
        capture_output=True,
        text=True,
    )
    wav_probe = None
    wav_verified = False
    wav_size = wav_path.stat().st_size if wav_path.exists() else 0
    if wav_path.exists() and wav_path.stat().st_size > 44:
        wav_probe = ffprobe_json(wav_path)
        wav_verified = has_standard_wav_stream(wav_probe) and wav_size > 4096
    return {
        "returncode": proc.returncode,
        "stderr": proc.stderr.strip(),
        "stdout": proc.stdout.strip(),
        "wav_size": wav_size,
        "wav_probe": wav_probe,
        "wav_verified": wav_verified,
    }


def extract_aligned_clip_wav(
    source_path: Path,
    wav_path: Path,
    start_sec: float,
    clip_seconds: float,
):
    proc = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-v",
            "warning",
            "-i",
            str(source_path),
            "-ss",
            f"{start_sec:.3f}",
            "-t",
            f"{clip_seconds:.3f}",
            "-map",
            "0:a:0",
            "-c:a",
            "pcm_s16le",
            str(wav_path),
        ],
        capture_output=True,
        text=True,
    )
    wav_probe = None
    wav_verified = False
    wav_size = wav_path.stat().st_size if wav_path.exists() else 0
    if wav_path.exists() and wav_size > 44:
        wav_probe = ffprobe_json(wav_path)
        wav_verified = has_standard_wav_stream(wav_probe) and wav_size > 4096
    return {
        "returncode": proc.returncode,
        "stderr": proc.stderr.strip(),
        "stdout": proc.stdout.strip(),
        "wav_size": wav_size,
        "wav_probe": wav_probe,
        "wav_verified": wav_verified,
    }


def clip_duration_ok(probe, clip_seconds: float, tolerance: float = 0.1):
    duration = best_probe_duration(probe or {})
    if duration is None:
        return False
    return duration >= max(0.0, clip_seconds - tolerance)


def download_source_webm_ytdlp(video_id: str, source_path: Path):
    cmd = [
        "yt-dlp",
        "-f",
        FOA_ITAG,
        "--no-part",
        "--no-progress",
        "-o",
        str(source_path),
        f"https://www.youtube.com/watch?v={video_id}",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    probe = ffprobe_json(source_path) if source_path.exists() else None
    source_verified = bool(probe and has_source_foa_stream(probe))
    return {
        "method": "ytdlp",
        "returncode": proc.returncode,
        "stdout": trim_text(proc.stdout),
        "stderr": trim_text(proc.stderr),
        "source_webm_path": str(source_path) if source_path.exists() else None,
        "source_probe": probe,
        "source_verified": source_verified,
    }


def source_artifacts_complete(existing: dict, clip_seconds: float):
    if existing.get("pipeline_version") != PIPELINE_VERSION:
        return False
    source_path = existing.get("source_webm_path")
    wav_path = existing.get("wav_path")
    if not source_path or not wav_path:
        return False
    source_file = Path(source_path)
    wav_file = Path(wav_path)
    if not source_file.exists() or not wav_file.exists():
        return False
    if wav_file.stat().st_size <= 4096:
        return False
    if not existing.get("wav_verified"):
        return False
    return clip_duration_ok(existing.get("wav_probe"), clip_seconds)


def candidate_rank(candidate):
    return (
        1 if candidate.get("wav_verified") else 0,
        candidate.get("wav_size") or 0,
        candidate.get("wav_duration") or -1.0,
        1 if candidate.get("probe_has_foa_stream") else 0,
        1 if candidate.get("probe_has_four_channel_audio") else 0,
        1 if candidate.get("probe_has_audio") else 0,
        candidate.get("probe_duration") or -1.0,
        -candidate.get("index", 0),
    )


def build_media_artifacts(sample_id: str, raw_path: Path, dirs):
    data = raw_path.read_bytes()
    ebml_offsets = find_ebml_offsets(data)
    candidates = []
    selected_candidate = None

    with tempfile.TemporaryDirectory(prefix=f"{sample_id}-wav-build-") as tmpdir:
        tmpdir = Path(tmpdir)
        for index, offset in enumerate(ebml_offsets):
            candidate_path = tmpdir / f"{sample_id}_{index}.webm"
            candidate_path.write_bytes(data[offset:])

            probe = ffprobe_json(candidate_path)
            probe_duration = best_probe_duration(probe)
            audio_index = select_audio_stream_index(probe)
            candidate = {
                "index": index,
                "offset": offset,
                "probe_has_audio": bool(audio_streams(probe)),
                "probe_has_four_channel_audio": has_four_channel_audio(probe),
                "probe_has_foa_stream": has_foa_stream(probe),
                "probe_duration": probe_duration,
                "best_audio_stream_index": audio_index,
            }

            wav_result = None
            if audio_index is not None:
                wav_tmp_path = tmpdir / f"{sample_id}_{index}.wav"
                wav_result = extract_standard_wav(candidate_path, wav_tmp_path)
                candidate["wav_verified"] = wav_result["wav_verified"]
                candidate["wav_size"] = wav_result["wav_size"]
                candidate["wav_duration"] = best_probe_duration(wav_result["wav_probe"] or {})
            else:
                candidate["wav_verified"] = False
                candidate["wav_size"] = 0
                candidate["wav_duration"] = None

            candidates.append(candidate)

            if selected_candidate is None or candidate_rank(candidate) > candidate_rank(selected_candidate):
                selected_candidate = {
                    **candidate,
                    "probe": probe,
                    "candidate_tmp_path": str(candidate_path),
                    "wav_tmp_path": str(tmpdir / f"{sample_id}_{index}.wav") if wav_result else None,
                    "wav_probe": None if wav_result is None else wav_result["wav_probe"],
                    "wav_size": 0 if wav_result is None else wav_result["wav_size"],
                    "wav_ffmpeg_returncode": None if wav_result is None else wav_result["returncode"],
                    "wav_ffmpeg_stderr": None
                    if wav_result is None
                    else wav_result["stderr"][-2000:],
                }

        carved_path = dirs["carved"] / f"{sample_id}.webm"
        wav_path = dirs["wav"] / f"{sample_id}.wav"
        carved_probe = None
        wav_probe = None
        selected_opus_head = None
        wav_verified = False

        if selected_candidate is not None:
            shutil.copyfile(selected_candidate["candidate_tmp_path"], carved_path)
            carved_probe = ffprobe_json(carved_path)
            selected_opus_head = parse_opus_head(carved_path.read_bytes())
            if selected_candidate.get("wav_verified"):
                final_wav_result = extract_standard_wav(carved_path, wav_path)
                wav_probe = final_wav_result["wav_probe"]
                wav_verified = final_wav_result["wav_verified"]
                if not wav_verified and wav_path.exists():
                    wav_path.unlink()
            elif wav_path.exists():
                wav_path.unlink()

    return {
        "raw_ump_path": str(raw_path),
        "raw_ump_size": raw_path.stat().st_size,
        "ebml_offsets": ebml_offsets,
        "candidate_count": len(candidates),
        "candidate_evaluations": candidates,
        "selected_candidate": None
        if selected_candidate is None
        else {
            "index": selected_candidate["index"],
            "offset": selected_candidate["offset"],
            "probe_duration": selected_candidate["probe_duration"],
            "probe_has_audio": selected_candidate["probe_has_audio"],
            "probe_has_four_channel_audio": selected_candidate["probe_has_four_channel_audio"],
            "probe_has_foa_stream": selected_candidate["probe_has_foa_stream"],
            "best_audio_stream_index": selected_candidate["best_audio_stream_index"],
            "wav_verified": selected_candidate["wav_verified"],
            "wav_size": selected_candidate["wav_size"],
            "wav_duration": selected_candidate["wav_duration"],
            "wav_ffmpeg_returncode": selected_candidate["wav_ffmpeg_returncode"],
            "wav_ffmpeg_stderr": selected_candidate["wav_ffmpeg_stderr"],
        },
        "carved_path": str(carved_path) if selected_candidate is not None else None,
        "carved_probe": carved_probe,
        "selected_opus_head": selected_opus_head,
        "wav_path": str(wav_path) if wav_verified else None,
        "wav_probe": wav_probe,
        "wav_verified": wav_verified,
    }


def merge_capture_and_artifacts(sample_id: str, existing: dict, artifact_info: dict):
    video_id, start_sec = split_sample_id(sample_id)
    result = dict(existing or {})
    result["sample_id"] = sample_id
    result["video_id"] = video_id
    result["clip_start_sec"] = start_sec
    result["pipeline_version"] = PIPELINE_VERSION
    result.update(artifact_info)

    format_meta = ((result.get("capture_state") or {}).get("formats") or [{}])[0]
    metadata_foa = (
        format_meta.get("audioChannels") == 4
        and format_meta.get("spatialAudioType") == "SPATIAL_AUDIO_TYPE_AMBISONICS_QUAD"
    )
    selected_opus_head = result.get("selected_opus_head")
    opus_foa = bool(selected_opus_head and selected_opus_head.get("channel_count") == 4)
    result["foa_verified"] = bool(
        result.get("wav_verified")
        or (result.get("carved_probe") and has_foa_stream(result["carved_probe"]))
        or (metadata_foa and opus_foa)
    )
    return result


def artifacts_complete(existing: dict):
    wav_path = existing.get("wav_path")
    if existing.get("pipeline_version") != PIPELINE_VERSION:
        return False
    if not existing.get("wav_verified") or not wav_path:
        return False
    wav_file = Path(wav_path)
    return wav_file.exists() and wav_file.stat().st_size > 4096


class CDPClient:
    def __init__(self, ws):
        self.ws = ws
        self._next_id = 1
        self._pending = {}
        self._events = []
        self._event_signal = asyncio.Event()
        self._reader_task = None

    async def start(self):
        self._reader_task = asyncio.create_task(self._reader())

    async def _reader(self):
        async for raw in self.ws:
            msg = json.loads(raw)
            if "id" in msg:
                fut = self._pending.pop(msg["id"], None)
                if fut and not fut.done():
                    fut.set_result(msg)
            else:
                self._events.append(msg)
                self._event_signal.set()

    async def call(self, method, params=None):
        msg_id = self._next_id
        self._next_id += 1
        fut = asyncio.get_event_loop().create_future()
        self._pending[msg_id] = fut
        payload = {"id": msg_id, "method": method}
        if params is not None:
            payload["params"] = params
        await self.ws.send(json.dumps(payload))
        return await fut

    async def eval_json(self, expression: str):
        result = await self.call(
            "Runtime.evaluate",
            {"expression": expression, "returnByValue": True, "awaitPromise": True},
        )
        value = result["result"]["result"].get("value")
        return json.loads(value) if isinstance(value, str) else value

    async def wait_for_event(self, predicate, since=0, timeout=30):
        deadline = time.time() + timeout
        cursor = since
        while time.time() < deadline:
            while cursor < len(self._events):
                event = self._events[cursor]
                cursor += 1
                if predicate(event):
                    return event
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            self._event_signal.clear()
            try:
                await asyncio.wait_for(self._event_signal.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                break
        raise TimeoutError("timed out waiting for matching CDP event")

    def mark(self):
        return len(self._events)


class BrowserCapture:
    def __init__(self, chrome_path: str, headless: bool):
        self.chrome_path = chrome_path
        self.headless = headless
        self.proc = None
        self.ws = None
        self.cdp = None
        self.port = None
        self.user_data_dir = None

    async def __aenter__(self):
        self.port = random.randint(30000, 50000)
        self.user_data_dir = tempfile.mkdtemp(prefix="yt-ambigen-foa-")
        cmd = [
            self.chrome_path,
            "--disable-gpu",
            "--no-first-run",
            "--no-default-browser-check",
            "--autoplay-policy=no-user-gesture-required",
            "--remote-debugging-address=127.0.0.1",
            f"--remote-debugging-port={self.port}",
            f"--user-data-dir={self.user_data_dir}",
            "about:blank",
        ]
        if self.headless:
            cmd.insert(1, "--headless=new")
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        session = requests.Session()
        session.trust_env = False
        tabs = None
        for _ in range(100):
            try:
                response = session.get(f"http://127.0.0.1:{self.port}/json/list", timeout=1)
                tabs = response.json()
                break
            except Exception:
                time.sleep(0.1)
        if not tabs:
            raise RuntimeError("Chrome CDP did not become ready")
        page_tabs = [tab for tab in tabs if tab.get("type") == "page"]
        if not page_tabs:
            raise RuntimeError("No page target exposed by Chrome CDP")
        self.ws = await websockets.connect(
            page_tabs[0]["webSocketDebuggerUrl"],
            open_timeout=10,
            max_size=None,
        )
        self.cdp = CDPClient(self.ws)
        await self.cdp.start()
        for method in ("Page.enable", "Runtime.enable", "Network.enable"):
            await self.cdp.call(method, {})
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if self.ws:
                await self.ws.close()
        finally:
            if self.proc and self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.proc.kill()

    async def get_page_state(self):
        expr = """
JSON.stringify((() => {
  const p = window.ytInitialPlayerResponse;
  if (!p) return {ready: false, href: location.href};
  const formats = (((p.streamingData || {}).adaptiveFormats) || [])
    .filter(f => String(f.itag) === "338")
    .map(f => ({
      itag: f.itag,
      audioChannels: f.audioChannels,
      spatialAudioType: f.spatialAudioType,
      mimeType: f.mimeType,
      approxDurationMs: f.approxDurationMs
    }));
  return {
    ready: true,
    href: location.href,
    title: p.videoDetails && p.videoDetails.title,
    playability: p.playabilityStatus && p.playabilityStatus.status,
    formats
  };
})())
""".strip()
        return await self.cdp.eval_json(expr)

    async def navigate_and_capture(self, video_id: str, start_sec: int, timeout: int):
        nav_mark = self.cdp.mark()
        await self.cdp.call("Page.navigate", {"url": f"https://www.youtube.com/watch?v={video_id}"})
        state = None
        for _ in range(timeout):
            state = await self.get_page_state()
            if state.get("ready") and state.get("formats"):
                break
            await asyncio.sleep(1)
        if not state or not state.get("ready"):
            raise RuntimeError("ytInitialPlayerResponse never became ready")
        if not state.get("formats"):
            raise RuntimeError("FOA itag 338 not present in adaptiveFormats")
        if state.get("playability") != "OK":
            raise RuntimeError(f"Player not playable: {state.get('playability')}")

        initial_request = await self.cdp.wait_for_event(
            lambda msg: msg.get("method") == "Network.requestWillBeSent"
            and "googlevideo.com/videoplayback" in msg.get("params", {}).get("request", {}).get("url", "")
            and "sabr=1" in msg.get("params", {}).get("request", {}).get("url", "")
            and msg.get("params", {}).get("request", {}).get("method") == "POST",
            since=nav_mark,
            timeout=timeout,
        )

        request_event = initial_request
        if start_sec > 0:
            seek_mark = self.cdp.mark()
            seek_expr = f"""
JSON.stringify((() => {{
  const v = document.querySelector('video');
  if (!v) return {{ok: false, reason: 'no_video'}};
  v.pause();
  v.currentTime = {start_sec};
  v.play().catch(() => null);
  return {{ok: true, currentTime: v.currentTime}};
}})())
""".strip()
            await self.cdp.eval_json(seek_expr)
            try:
                request_event = await self.cdp.wait_for_event(
                    lambda msg: msg.get("method") == "Network.requestWillBeSent"
                    and "googlevideo.com/videoplayback" in msg.get("params", {}).get("request", {}).get("url", "")
                    and "sabr=1" in msg.get("params", {}).get("request", {}).get("url", "")
                    and msg.get("params", {}).get("request", {}).get("method") == "POST",
                    since=seek_mark,
                    timeout=min(timeout, 12),
                )
            except TimeoutError:
                request_event = initial_request

        requests_to_replay = []
        for event in (initial_request, request_event):
            req_params = event["params"]
            request_id = req_params["requestId"]
            post_data_result = await self.cdp.call("Network.getRequestPostData", {"requestId": request_id})
            req_url = req_params["request"]["url"]
            if any(item["event"]["request"]["url"] == req_url for item in requests_to_replay):
                continue
            requests_to_replay.append(
                {
                    "event": req_params,
                    "post_data_result": post_data_result["result"],
                }
            )
        return {
            "state": state,
            "requests_to_replay": requests_to_replay,
        }


def replay_sabr_request(url: str, post_data: str, base64_encoded: bool, request_headers: dict):
    body = base64.b64decode(post_data) if base64_encoded else post_data.encode("utf-8")
    headers = {}
    for key in ("Content-Type", "Origin", "Referer", "User-Agent"):
        if key in request_headers:
            headers[key] = request_headers[key]
    if "Content-Type" not in headers:
        headers["Content-Type"] = "application/octet-stream"
    last_error = None
    for trust_env in (True, False):
        session = requests.Session()
        session.trust_env = trust_env
        try:
            response = session.post(url, data=body, headers=headers, timeout=(15, 60))
            response.raise_for_status()
            return {
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "body": response.content,
                "trust_env": trust_env,
            }
        except Exception as exc:
            last_error = exc
    raise last_error


async def process_sample(sample_id: str, args, dirs):
    video_id, start_sec = split_sample_id(sample_id)
    meta_path = dirs["meta"] / f"{sample_id}.json"
    raw_path = dirs["raw_ump"] / f"{sample_id}.bin"

    vlog(args, f"[{sample_id}] open watch page")
    async with BrowserCapture(args.chrome_path, args.headless) as browser:
        capture = await browser.navigate_and_capture(video_id, start_sec, args.timeout)
    vlog(args, f"[{sample_id}] got SABR request for FOA")

    replayed_parts = []
    combined_body = b""
    request_urls = []
    request_headers = []
    post_lengths = []
    for item in capture["requests_to_replay"]:
        request_info = item["event"]["request"]
        replay = replay_sabr_request(
            request_info["url"],
            item["post_data_result"]["postData"],
            bool(item["post_data_result"].get("base64Encoded")),
            request_info.get("headers", {}),
        )
        combined_body += replay["body"]
        replayed_parts.append(replay)
        request_urls.append(request_info["url"])
        request_headers.append(request_info.get("headers", {}))
        post_lengths.append(len(item["post_data_result"]["postData"]))
    vlog(args, f"[{sample_id}] replayed {len(replayed_parts)} UMP part(s) {len(combined_body)} bytes")
    raw_path.write_bytes(combined_body)

    artifact_info = build_media_artifacts(sample_id, raw_path, dirs)
    result = merge_capture_and_artifacts(
        sample_id,
        {
            "capture_state": capture["state"],
            "request_urls": request_urls,
            "request_headers": request_headers,
            "request_postData_lengths": post_lengths,
            "replay_status_codes": [part["status_code"] for part in replayed_parts],
            "replay_headers": [part["headers"] for part in replayed_parts],
        },
        artifact_info,
    )
    meta_path.write_text(json.dumps(result, indent=2))
    vlog(
        args,
        f"[{sample_id}] foa_verified={result['foa_verified']} wav_verified={result.get('wav_verified')}",
    )
    return result


def process_sample_ytdlp(sample_id: str, args, dirs):
    video_id, start_sec = split_sample_id(sample_id)
    meta_path = dirs["meta"] / f"{sample_id}.json"
    source_path = dirs["source_webm"] / f"{video_id}.webm"
    wav_path = dirs["wav"] / f"{sample_id}.wav"

    if source_path.exists():
        source_probe = ffprobe_json(source_path)
        if has_source_foa_stream(source_probe):
            source_info = {
                "method": "ytdlp",
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "source_webm_path": str(source_path),
                "source_probe": source_probe,
                "source_verified": True,
            }
        else:
            source_info = download_source_webm_ytdlp(video_id, source_path)
    else:
        source_info = download_source_webm_ytdlp(video_id, source_path)

    if not source_info["source_verified"]:
        raise RuntimeError(
            f"yt-dlp did not yield a verified FOA source for {video_id}: rc={source_info['returncode']}"
        )

    wav_result = extract_aligned_clip_wav(source_path, wav_path, start_sec, args.clip_seconds)
    if not wav_result["wav_verified"]:
        raise RuntimeError(
            f"ffmpeg failed to export aligned WAV for {sample_id}: rc={wav_result['returncode']}"
        )

    result = {
        "pipeline_version": PIPELINE_VERSION,
        "sample_id": sample_id,
        "video_id": video_id,
        "clip_start_sec": start_sec,
        "clip_seconds": args.clip_seconds,
        "download_method": "ytdlp",
        "source_webm_path": source_info["source_webm_path"],
        "source_probe": source_info["source_probe"],
        "source_verified": source_info["source_verified"],
        "ytdlp_returncode": source_info["returncode"],
        "ytdlp_stdout": source_info["stdout"],
        "ytdlp_stderr": source_info["stderr"],
        "wav_path": str(wav_path),
        "wav_probe": wav_result["wav_probe"],
        "wav_verified": wav_result["wav_verified"],
        "wav_size": wav_result["wav_size"],
        "wav_ffmpeg_returncode": wav_result["returncode"],
        "wav_ffmpeg_stdout": trim_text(wav_result["stdout"]),
        "wav_ffmpeg_stderr": trim_text(wav_result["stderr"]),
        "clip_aligned_verified": clip_duration_ok(wav_result["wav_probe"], args.clip_seconds),
        "foa_verified": bool(source_info["source_probe"] and has_source_foa_stream(source_info["source_probe"])),
    }
    meta_path.write_text(json.dumps(result, indent=2))
    vlog(
        args,
        f"[{sample_id}] ytdlp source ok; foa_verified={result['foa_verified']} wav_verified={result['wav_verified']}",
    )
    return result


def rebuild_from_existing_source(sample_id: str, args, dirs, existing: Optional[dict] = None):
    video_id, start_sec = split_sample_id(sample_id)
    meta_path = dirs["meta"] / f"{sample_id}.json"
    source_path = dirs["source_webm"] / f"{video_id}.webm"
    wav_path = dirs["wav"] / f"{sample_id}.wav"
    if not source_path.exists():
        raise FileNotFoundError(f"source webm not found: {source_path}")

    source_probe = ffprobe_json(source_path)
    if not has_source_foa_stream(source_probe):
        raise RuntimeError(f"cached source is not verified FOA: {source_path}")

    wav_result = extract_aligned_clip_wav(source_path, wav_path, start_sec, args.clip_seconds)
    if not wav_result["wav_verified"]:
        raise RuntimeError(
            f"ffmpeg failed to export aligned WAV for {sample_id}: rc={wav_result['returncode']}"
        )

    result = dict(existing or {})
    result.update(
        {
            "pipeline_version": PIPELINE_VERSION,
            "sample_id": sample_id,
            "video_id": video_id,
            "clip_start_sec": start_sec,
            "clip_seconds": args.clip_seconds,
            "download_method": result.get("download_method") or "ytdlp",
            "source_webm_path": str(source_path),
            "source_probe": source_probe,
            "source_verified": True,
            "wav_path": str(wav_path),
            "wav_probe": wav_result["wav_probe"],
            "wav_verified": wav_result["wav_verified"],
            "wav_size": wav_result["wav_size"],
            "wav_ffmpeg_returncode": wav_result["returncode"],
            "wav_ffmpeg_stdout": trim_text(wav_result["stdout"]),
            "wav_ffmpeg_stderr": trim_text(wav_result["stderr"]),
            "clip_aligned_verified": clip_duration_ok(wav_result["wav_probe"], args.clip_seconds),
            "foa_verified": True,
        }
    )
    meta_path.write_text(json.dumps(result, indent=2))
    vlog(
        args,
        f"[{sample_id}] rebuilt from source_webm; foa_verified={result['foa_verified']} wav_verified={result['wav_verified']}",
    )
    return result


def rebuild_from_existing_raw(sample_id: str, args, dirs, existing: Optional[dict] = None):
    meta_path = dirs["meta"] / f"{sample_id}.json"
    raw_path = dirs["raw_ump"] / f"{sample_id}.bin"
    if not raw_path.exists():
        raise FileNotFoundError(f"raw UMP not found: {raw_path}")
    vlog(args, f"[{sample_id}] rebuilding final webm/wav from existing raw_ump")
    artifact_info = build_media_artifacts(sample_id, raw_path, dirs)
    result = merge_capture_and_artifacts(sample_id, existing or {}, artifact_info)
    meta_path.write_text(json.dumps(result, indent=2))
    vlog(
        args,
        f"[{sample_id}] rebuilt foa_verified={result['foa_verified']} wav_verified={result.get('wav_verified')}",
    )
    return result


async def main_async():
    args = parse_args()
    csv_path = Path(args.csv)
    outdir = Path(args.outdir)
    dirs = ensure_dirs(outdir)

    all_ids = load_sample_ids(csv_path)
    selected = all_ids[args.offset : args.offset + args.limit]
    if not selected:
        raise SystemExit("No sample ids selected")

    manifest_path = dirs["reports"] / "manifest.jsonl"
    summary_path = dirs["reports"] / "summary.json"

    results = []
    for sample_id in selected:
        meta_path = dirs["meta"] / f"{sample_id}.json"
        video_id, _ = split_sample_id(sample_id)
        source_path = dirs["source_webm"] / f"{video_id}.webm"
        raw_path = dirs["raw_ump"] / f"{sample_id}.bin"
        if meta_path.exists():
            existing = json.loads(meta_path.read_text())
            if source_artifacts_complete(existing, args.clip_seconds):
                results.append(existing)
                continue
            if source_path.exists():
                try:
                    result = rebuild_from_existing_source(sample_id, args, dirs, existing)
                except Exception as exc:
                    result = dict(existing)
                    result["error"] = f"{type(exc).__name__}: {exc}"
                    meta_path.write_text(json.dumps(result, indent=2))
                results.append(result)
                continue
            if raw_path.exists():
                try:
                    result = rebuild_from_existing_raw(sample_id, args, dirs, existing)
                except Exception as exc:
                    result = dict(existing)
                    result["error"] = f"{type(exc).__name__}: {exc}"
                    meta_path.write_text(json.dumps(result, indent=2))
                results.append(result)
                continue

        if source_path.exists():
            try:
                result = rebuild_from_existing_source(sample_id, args, dirs)
            except Exception as exc:
                result = {
                    "pipeline_version": PIPELINE_VERSION,
                    "sample_id": sample_id,
                    "error": f"{type(exc).__name__}: {exc}",
                    "foa_verified": False,
                    "wav_verified": False,
                }
                meta_path.write_text(json.dumps(result, indent=2))
            results.append(result)
            continue

        if raw_path.exists():
            try:
                result = rebuild_from_existing_raw(sample_id, args, dirs)
            except Exception as exc:
                result = {
                    "sample_id": sample_id,
                    "error": f"{type(exc).__name__}: {exc}",
                    "foa_verified": False,
                    "wav_verified": False,
                }
                meta_path.write_text(json.dumps(result, indent=2))
            results.append(result)
            continue

        last_error = None
        result = None
        for attempt in range(1, args.retries + 1):
            try:
                vlog(args, f"[{sample_id}] attempt {attempt}/{args.retries}")
                if args.download_method in {"auto", "ytdlp"}:
                    try:
                        result = process_sample_ytdlp(sample_id, args, dirs)
                    except Exception as exc:
                        if args.download_method == "ytdlp":
                            raise
                        vlog(args, f"[{sample_id}] yt-dlp path failed, falling back to SABR: {exc}")
                        result = await process_sample(sample_id, args, dirs)
                else:
                    result = await process_sample(sample_id, args, dirs)
                break
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                vlog(args, f"[{sample_id}] failed: {last_error}")
                await asyncio.sleep(1)
        if result is None:
            result = {
                "sample_id": sample_id,
                "error": last_error,
                "foa_verified": False,
                "wav_verified": False,
            }
            meta_path.write_text(json.dumps(result, indent=2))
        results.append(result)

    manifest_path.write_text("".join(json.dumps(result) + "\n" for result in results))
    summary = {
        "csv": str(csv_path),
        "outdir": str(outdir),
        "pipeline_version": PIPELINE_VERSION,
        "clip_seconds": args.clip_seconds,
        "download_method": args.download_method,
        "selected_count": len(selected),
        "processed_count": len(results),
        "foa_verified_count": sum(1 for item in results if item.get("foa_verified")),
        "wav_verified_count": sum(1 for item in results if item.get("wav_verified")),
        "clip_aligned_count": sum(1 for item in results if item.get("clip_aligned_verified")),
        "failed_count": sum(1 for item in results if item.get("error")),
        "results": results,
    }
    summary_path.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
