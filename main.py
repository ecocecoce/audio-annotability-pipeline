#!/usr/bin/env python3
"""主入口脚本 - 支持视频+外部分离音频"""

import argparse
import json
from pathlib import Path
from tqdm import tqdm

from pipeline import CleaningPipeline
from utils import save_json
from config import CONFIG


def batch_process(video_dir: Path, audio_dir: Path, output_dir: Path, limit: int = None):
    """
    批量处理视频+外部分离的4通道音频
    
    文件命名需一致：
    - video_dir/clip_id.mp4
    - audio_dir/clip_id.wav
    """
    
    pipeline = CleaningPipeline(CONFIG)
    
    # 查找所有视频
    video_files = list(video_dir.glob("*.mp4"))
    video_files.sort()
    
    if limit:
        video_files = video_files[:limit]
    
    print(f"Found {len(video_files)} videos")
    print(f"Looking for matching WAV in: {audio_dir}")
    print("=" * 60)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 统计
    stats = {'high_quality': 0, 'weak_quality': 0, 'rejected': 0, 'no_audio': 0}
    
    for video_path in tqdm(video_files, desc="Processing"):
        clip_id = video_path.stem
        
        # 查找对应的音频
        audio_path = audio_dir / f"{clip_id}.wav"
        
        has_external_audio = audio_path.exists()
        if not has_external_audio:
            stats['no_audio'] += 1
            audio_path = None  # 将使用视频内音频
        
        try:
            # 传入音视频路径
            result = pipeline.process(video_path, audio_path)
            
            # 保存 JSON
            output_file = output_dir / f"{clip_id}.json"
            save_json(result, output_file)
            
            # 统计
            tier = result['routing']['tier']
            stats[tier] += 1
            
            # 打印第一个示例
            if video_path == video_files[0]:
                print(f"\n示例输出 ({clip_id}):")
                print(json.dumps(result, indent=2, ensure_ascii=False)[:1500])
                print("...\n")
            
        except Exception as e:
            print(f"\nError processing {clip_id}: {e}")
            continue
    
    # 保存统计
    stats_file = output_dir / "_statistics.json"
    with open(stats_file, 'w') as f:
        json.dump({
            'total': len(video_files),
            'stats': stats,
            'config': CONFIG
        }, f, indent=2)
    
    print(f"\n{'=' * 60}")
    print(f"Done!")
    print(f"  Total: {len(video_files)}")
    print(f"  High quality: {stats['high_quality']}")
    print(f"  Weak quality: {stats['weak_quality']}")
    print(f"  Rejected: {stats['rejected']}")
    print(f"  No external audio: {stats['no_audio']}")
    print(f"Output dir: {output_dir}")
    
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YT-Ambigen Data Cleaning (Video + External 4ch WAV)")
    parser.add_argument("--video_dir", required=True, help="视频文件目录 (*.mp4)")
    parser.add_argument("--audio_dir", required=True, help="4通道WAV文件目录 (*.wav)")
    parser.add_argument("--output_dir", required=True, help="输出JSON目录")
    parser.add_argument("--limit", type=int, default=None, help="处理数量限制")
    
    args = parser.parse_args()
    
    batch_process(
        Path(args.video_dir),
        Path(args.audio_dir),
        Path(args.output_dir),
        args.limit
    )