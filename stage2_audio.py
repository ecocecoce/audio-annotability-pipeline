"""Stage 2: 音频有效性筛选"""

import numpy as np
import librosa
from pathlib import Path
from typing import Dict


class AudioScreener:
    """基于信号处理的音频分析"""
    
    def __init__(self, config: dict):
        self.config = config['stage2']
    
    def screen(self, video_path: Path, media_check: Dict) -> Dict:
        """分析视频内的音频"""
        result = self._create_result('internal')
        
        try:
            from utils import extract_audio_4ch
            audio, sr = extract_audio_4ch(video_path)
            W = audio[:, 0]
            self._analyze(W, sr, result)
        except Exception as e:
            result['audio_screening']['error'] = str(e)[:50]
        
        return result
    
    def screen_wav(self, wav_path: Path) -> Dict:
        """直接分析WAV文件（外部4通道音频）"""
        result = self._create_result('external_wav')
        
        try:
            from utils import read_wav_4ch
            audio, sr = read_wav_4ch(wav_path)
            
            # 使用W通道（第0通道）
            W = audio[:, 0] if audio.shape[1] >= 4 else audio.mean(axis=1)
            self._analyze(W, sr, result)
        except Exception as e:
            result['audio_screening']['error'] = str(e)[:50]
        
        return result
    
    def _create_result(self, source: str) -> Dict:
        return {
            'audio_screening': {
                'event_density': 0.0,
                'speech_presence': False,
                'speech_ratio': 0.0,
                'bgm_risk': 0.0,
                'voiceover_risk': 0.0,
                'source': source
            }
        }
    
    def _analyze(self, W: np.ndarray, sr: int, result: Dict):
        """执行音频分析"""
        # 事件密度
        event_density = self._compute_event_density(W, sr)
        result['audio_screening']['event_density'] = float(event_density)
        
        # 语音检测
        speech_info = self._detect_speech(W, sr)
        result['audio_screening']['speech_presence'] = speech_info['has_speech']
        result['audio_screening']['speech_ratio'] = float(speech_info['ratio'])
        
        # BGM 风险
        bgm_risk = self._estimate_bgm_risk(W, sr)
        result['audio_screening']['bgm_risk'] = float(bgm_risk)
        
        # 旁白风险
        if speech_info['ratio'] > 0.7 and event_density < 0.5:
            result['audio_screening']['voiceover_risk'] = 0.8
        else:
            result['audio_screening']['voiceover_risk'] = 0.0
    
    def _compute_event_density(self, audio: np.ndarray, sr: int) -> float:
        """计算事件密度"""
        hop = int(0.02 * sr)
        energy = librosa.feature.rms(y=audio, hop_length=hop)[0]
        
        spec = np.abs(librosa.stft(audio, hop_length=hop))
        flux = np.sum(np.diff(spec, axis=1)**2, axis=0)
        flux = np.concatenate([[0], flux])
        
        e_norm = (energy - energy.min()) / (energy.max() - energy.min() + 1e-10)
        f_norm = (flux - flux.min()) / (flux.max() - flux.min() + 1e-10)
        score = 0.6 * e_norm + 0.4 * f_norm
        
        is_event = score > 0.3
        return float(np.mean(is_event))
    
    def _detect_speech(self, audio: np.ndarray, sr: int) -> Dict:
        """检测语音"""
        hop = int(0.02 * sr)
        frames = len(audio) // hop
        
        speech_frames = 0
        for i in range(frames):
            frame = audio[i*hop:(i+1)*hop]
            energy = np.sqrt(np.mean(frame**2))
            if energy < 0.01:
                continue
            
            spec = np.abs(np.fft.rfft(frame))
            low_freq = np.sum(spec[:len(spec)//4])
            total = np.sum(spec) + 1e-10
            harmonic_ratio = low_freq / total
            
            if harmonic_ratio > 0.5:
                speech_frames += 1
        
        ratio = speech_frames / frames if frames > 0 else 0
        return {'has_speech': ratio > 0.05, 'ratio': ratio}
    
    def _estimate_bgm_risk(self, audio: np.ndarray, sr: int) -> float:
        """估计 BGM 风险"""
        flatness = librosa.feature.spectral_flatness(y=audio)[0]
        mean_flat = np.mean(flatness)
        risk = np.clip((mean_flat - 0.3) * 2, 0, 1)
        return float(risk)