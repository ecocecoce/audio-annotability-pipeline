"""工具函数"""

import tempfile
from pathlib import Path
import numpy as np
import soundfile as sf
from imageio_ffmpeg import get_ffmpeg_exe

# 获取 FFmpeg 路径
FFMPEG_EXE = get_ffmpeg_exe()

def extract_audio_4ch(video_path: Path) -> tuple:
    """从视频提取 4 通道音频"""
    with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp:
        tmp_path = tmp.name
    
    try:
        import subprocess
        # 使用明确的 FFmpeg 路径
        cmd = [
            FFMPEG_EXE,
            '-i', str(video_path),
            '-ac', '4', '-ar', '48000', '-acodec', 'pcm_s16le', '-vn',
            '-y', tmp_path
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        
        audio, sr = sf.read(tmp_path)
        if len(audio.shape) == 1:
            audio = audio.reshape(-1, 1)
        return audio, sr
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def save_json(result: dict, output_path: Path):
    """保存结果为 JSON"""
    import json
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)


def read_wav_4ch(wav_path: Path) -> tuple:
    """读取 4 通道 WAV 文件"""
    audio, sr = sf.read(str(wav_path))
    if len(audio.shape) == 1:
        audio = audio.reshape(-1, 1)
    return audio, sr