#!/usr/bin/env python3
"""
裁剪视频为前 5 秒并移除音频
"""

import os
import argparse
from pathlib import Path
import subprocess
from imageio_ffmpeg import get_ffmpeg_exe

# 获取 ffmpeg 路径
FFMPEG_EXE = get_ffmpeg_exe()

def crop_to_5s_no_audio(input_path: Path, output_dir: Path):
    """裁剪视频为前 5 秒，移除音频"""
    output_file = output_dir / f"{input_path.stem}_5s.mp4"
    
    cmd = [
        FFMPEG_EXE,  # <-- 使用获取到的路径，而不是 "ffmpeg"
        "-y",
        "-i", str(input_path),
        "-t", "5",
        "-c:v", "copy",
        "-an",
        str(output_file)
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"✅ {input_path.name} -> {output_file.name}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed: {input_path.name}")
        print(f"   Error: {e.stderr[:200]}")
        return False


def batch_process(input_dir: Path, output_dir: Path):
    """批量处理"""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    video_files = list(input_dir.glob("*.mp4"))
    video_files.sort()
    
    print(f"Found {len(video_files)} videos")
    print(f"FFmpeg: {FFMPEG_EXE}")  # 打印路径确认
    print(f"Output: {output_dir}")
    print("=" * 50)
    
    success = 0
    failed = 0
    
    for video_path in video_files:
        if crop_to_5s_no_audio(video_path, output_dir):
            success += 1
        else:
            failed += 1
    
    print("=" * 50)
    print(f"Done: {success} success, {failed} failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crop videos to first 5s, no audio")
    parser.add_argument("--input_dir", required=True, help="输入视频目录")
    parser.add_argument("--output_dir", required=True, help="输出目录")
    
    args = parser.parse_args()
    
    batch_process(Path(args.input_dir), Path(args.output_dir))