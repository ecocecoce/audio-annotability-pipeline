"""Stage 1: 媒体完整性检查"""

import subprocess
import json
from pathlib import Path
from typing import Dict

import cv2
from utils import FFMPEG_EXE


class MediaChecker:
    """检查媒体文件完整性"""
    
    def __init__(self, config: dict):
        self.config = config['stage1']
        self.expected_channels = config['dataset']['expected_audio_channels']
        self.expected_sr = config['dataset']['expected_sample_rate']
    
    def check(self, video_path: Path, external_audio_path: Path = None) -> Dict:
        result = {
            'clip_id': video_path.stem,
            'media_check': {
                'decode_ok': False,
                'fps': None,
                'sample_rate': None,
                'num_channels': None,
                'silence_ratio': None,
                'clipping_ratio': None,
                'duration': None,
                'audio_source': 'internal',
                'external_audio_channels': None,
                'external_audio_sr': None,
                'has_external_foa': False,
                'error_msg': None
            }
        }
        
        try:
            # 使用 OpenCV 读取视频信息（不需要 ffprobe）
            cap = cv2.VideoCapture(str(video_path))
            if not cap.isOpened():
                result['media_check']['error_msg'] = "Cannot open video with OpenCV"
                return result
            
            fps = cap.get(cv2.CAP_PROP_FPS)
            total_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
            duration = total_frames / fps if fps > 0 else 0
            
            result['media_check']['fps'] = fps
            result['media_check']['duration'] = duration
            
            cap.release()
            
            # 检查外部音频（如果提供）
            if external_audio_path and external_audio_path.exists():
                audio_info = self._verify_external_audio(external_audio_path)
                result['media_check']['audio_source'] = 'external_wav'
                result['media_check']['external_audio_channels'] = audio_info.get('channels')
                result['media_check']['external_audio_sr'] = audio_info.get('sample_rate')
                
                if audio_info.get('channels') == 4:
                    result['media_check']['num_channels'] = 4
                    result['media_check']['sample_rate'] = audio_info.get('sample_rate')
                    result['media_check']['has_external_foa'] = True
                    result['media_check']['decode_ok'] = True
                    return result
            
            # 没有外部音频，检查视频内部音频（提取分析）
            result['media_check']['num_channels'] = 0  # 不知道，需要提取
            result['media_check']['sample_rate'] = 0
            
            # 尝试提取音频分析（如果失败也没关系）
            try:
                audio_stats = self._analyze_audio(video_path)
                result['media_check'].update(audio_stats)
            except Exception as e:
                result['media_check']['audio_extract_error'] = str(e)[:50]
                # 如果没有外部音频且提取失败，标记为失败
                result['media_check']['error_msg'] = "No external audio and cannot extract internal"
                return result
            
            # 验证 FOA 格式
            if result['media_check'].get('num_channels') != self.expected_channels:
                result['media_check']['error_msg'] = (
                    f"Not FOA: {result['media_check'].get('num_channels', 0)} channels"
                )
                return result
            
            result['media_check']['decode_ok'] = True
            
        except Exception as e:
            result['media_check']['error_msg'] = str(e)[:100]
        
        return result
    
    def _verify_external_audio(self, audio_path: Path) -> dict:
        """验证外部音频文件"""
        import soundfile as sf
        info = sf.info(str(audio_path))
        return {
            'channels': info.channels,
            'sample_rate': info.samplerate,
            'duration': info.duration,
        }
    
    def _analyze_audio(self, video_path: Path) -> Dict:
        """分析音频质量（使用 FFmpeg 提取）"""
        import tempfile
        import numpy as np
        import soundfile as sf
        
        # 使用 FFMPEG_EXE 提取音频
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            cmd = [
                FFMPEG_EXE,
                '-i', str(video_path),
                '-vn',  # 无视频
                '-ac', '2',  # 转为 2 通道（因为是纯视频文件，可能没有音频）
                '-ar', '48000',
                '-acodec', 'pcm_s16le',
                '-y', tmp_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0 or not Path(tmp_path).exists():
                raise RuntimeError(f"FFmpeg failed: {result.stderr[:100]}")
            
            audio, sr = sf.read(tmp_path)
            if len(audio.shape) == 1:
                audio = audio.reshape(-1, 1)
            
            # 更新信息
            return {
                'sample_rate': sr,
                'num_channels': audio.shape[1] if len(audio.shape) > 1 else 1,
                'silence_ratio': 0.0,  # 简化
                'clipping_ratio': 0.0,
                'max_amplitude': float(np.max(np.abs(audio))) if audio.size > 0 else 0
            }
            
        finally:
            Path(tmp_path).unlink(missing_ok=True)