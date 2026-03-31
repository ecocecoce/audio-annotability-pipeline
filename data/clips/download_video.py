import os
import pandas as pd
import subprocess
import argparse
import time
from pathlib import Path

def parse_clip_id(clip_id):
    parts = clip_id.split("_")
    video_id = "_".join(parts[:-1])
    start = float(parts[-1])
    return video_id, start


def download_video_only(clip_id, duration, out_dir):
    video_id, start = parse_clip_id(clip_id)
    url = f"https://www.youtube.com/watch?v={video_id}"
    output_file = os.path.join(out_dir, f"{clip_id}.mp4")
    
    # 方案1: 标准纯视频下载（最干净）
    cmd1 = [
        "yt-dlp",
        "--cookies-from-browser", "firefox",
        "--extractor-args", "youtube:player_client=web",
        "-f", "bestvideo[ext=mp4]/bestvideo[ext=webm]/bestvideo/bv*",  # 只选视频流
        "--downloader", "ffmpeg",  # 使用ffmpeg处理分段
        "--downloader-args", f"ffmpeg:-ss {start} -t {duration} -c:v copy -an",  # -an = 无音频
        "--merge-output-format", "mp4",
        "-o", output_file,
        "--no-keep-video",  # 不保留临时文件
        "--retries", "10",
        "--fragment-retries", "10",
        "--sleep-interval", "2",
        url
    ]
    
    # 方案2: 如果方案1失败，尝试获取URL用ffmpeg直接拉取
    cmd2_geturl = [
        "yt-dlp",
        "--cookies-from-browser", "firefox",
        "-f", "bestvideo[ext=mp4]/bestvideo/bv*",
        "--get-url",  # 只获取URL
        url
    ]
    
    for attempt in range(3):
        try:
            print(f"🎬 {clip_id} (Attempt {attempt+1}/3)")
            
            # 先尝试方案1
            result = subprocess.run(cmd1, capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0 and os.path.exists(output_file):
                # 验证是否真的是视频且无音频
                probe = subprocess.run(
                    ["ffprobe", "-v", "error", "-show_entries", 
                     "stream=codec_type", "-of", "csv=p=0", output_file],
                    capture_output=True, text=True
                )
                if "video" in probe.stdout and "audio" not in probe.stdout:
                    print(f"✅ Success (video only): {clip_id}")
                    return True
                elif "audio" in probe.stdout:
                    # 有音频，用ffmpeg移除
                    temp_file = output_file + ".temp.mp4"
                    subprocess.run([
                        "ffmpeg", "-y", "-i", output_file,
                        "-c:v", "copy", "-an",  # 复制视频，去掉音频
                        temp_file
                    ], check=True, capture_output=True)
                    os.replace(temp_file, output_file)
                    print(f"✅ Success (audio stripped): {clip_id}")
                    return True
            
            # 方案1失败，尝试方案2（获取URL手动下载）
            print(f"   尝试备用方案...")
            url_result = subprocess.run(cmd2_geturl, capture_output=True, text=True)
            if url_result.returncode == 0:
                video_url = url_result.stdout.strip().split('\n')[0]
                if video_url.startswith('http'):
                    # 用ffmpeg直接下载片段
                    ffmpeg_cmd = [
                        "ffmpeg", "-y",
                        "-ss", str(start),
                        "-i", video_url,
                        "-t", str(duration),
                        "-c:v", "copy", "-an",  # 无音频
                        "-bsf:a", "aac_adtstoasc",
                        output_file
                    ]
                    subprocess.run(ffmpeg_cmd, check=True, capture_output=True, timeout=120)
                    if os.path.exists(output_file):
                        print(f"✅ Success (ffmpeg direct): {clip_id}")
                        return True
            
            print(f"   失败，重试...")
            time.sleep(3)
            
        except subprocess.TimeoutExpired:
            print(f"   超时")
        except Exception as e:
            print(f"   错误: {str(e)[:100]}")
    
    print(f"❌ Failed: {clip_id}")
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv_path", type=str, required=True)
    parser.add_argument("--output_dir", type=str, default="./video_only")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=10)
    parser.add_argument("--duration", type=float, default=5.0)

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    df = pd.read_csv(args.csv_path)
    clip_ids = df.iloc[:, 0].tolist()[args.start:args.end]

    success_count = 0
    for clip_id in clip_ids:
        if download_video_only(clip_id, args.duration, args.output_dir):
            success_count += 1
        time.sleep(2)  # 避免请求过快
    
    print(f"\n完成: {success_count}/{len(clip_ids)} 成功")


if __name__ == "__main__":
    main()