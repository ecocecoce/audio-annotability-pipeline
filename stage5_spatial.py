"""Stage 5: 空间有效性筛选"""

import numpy as np
from pathlib import Path
from typing import Dict


class SpatialScreener:
    """FOA 空间分析"""
    
    def __init__(self, config: dict):
        self.config = config['stage5']
    
    def screen(self, video_path: Path, media_check: Dict) -> Dict:
        """分析视频内的音频空间信息"""
        result = self._create_result('internal')
        
        try:
            from utils import extract_audio_4ch
            audio, sr = extract_audio_4ch(video_path)
            W, X, Y, Z = audio[:, 0], audio[:, 1], audio[:, 2], audio[:, 3]
            self._analyze_directions(W, X, Y, Z, sr, result)
        except Exception as e:
            result['spatial_screening']['error'] = str(e)[:50]
        
        return result
    
    def screen_wav(self, wav_path: Path) -> Dict:
        """直接分析WAV文件的空间信息"""
        result = self._create_result('external_wav')
        
        try:
            from utils import read_wav_4ch
            audio, sr = read_wav_4ch(wav_path)
            
            if audio.shape[1] != 4:
                result['spatial_screening']['error'] = f"Not 4ch: {audio.shape[1]} channels"
                return result
            
            W, X, Y, Z = audio[:, 0], audio[:, 1], audio[:, 2], audio[:, 3]
            self._analyze_directions(W, X, Y, Z, sr, result)
        except Exception as e:
            result['spatial_screening']['error'] = str(e)[:50]
        
        return result
    
    def _create_result(self, source: str) -> Dict:
        return {
            'spatial_screening': {
                'spatial_validity_score': 0.0,
                'direction_consistency': 0.0,
                'has_valid_direction': False,
                'source': source
            }
        }
    
    def _analyze_directions(self, W, X, Y, Z, sr, result: Dict):
        """计算方向时间线并分析"""
        direction_traj = self._compute_directions(W, X, Y, Z, sr)
        
        if len(direction_traj) > 0:
            azimuths = [d['azimuth'] for d in direction_traj 
                       if d['azimuth'] is not None]
            
            if len(azimuths) > 3:
                diffs = np.diff(azimuths)
                mean_diff = np.mean(np.abs(diffs))
                std_diff = np.std(diffs)
                
                if 0.1 < mean_diff < 2.0 and std_diff < 1.0:
                    consistency = 0.8
                else:
                    consistency = 0.4
                
                validity = consistency * 0.8 + 0.2
                
                result['spatial_screening'].update({
                    'spatial_validity_score': float(validity),
                    'direction_consistency': float(consistency),
                    'has_valid_direction': True,
                })
    
    def _compute_directions(self, W, X, Y, Z, sr, window_ms=100):
        """计算方向时间线"""
        window = int(window_ms / 1000 * sr)
        hop = window // 2
        
        traj = []
        for start in range(0, len(W) - window, hop):
            w = np.mean(W[start:start+window])
            x = np.mean(X[start:start+window])
            y = np.mean(Y[start:start+window])
            z = np.mean(Z[start:start+window])
            
            energy = np.sqrt(x**2 + y**2 + z**2)
            if energy > 1e-6:
                azimuth = np.arctan2(y, x)
                elevation = np.arctan2(z, np.sqrt(x**2 + y**2))
                traj.append({
                    'time': start / sr,
                    'azimuth': float(np.degrees(azimuth)),
                    'elevation': float(np.degrees(elevation)),
                    'energy': float(energy),
                })
            else:
                traj.append({'time': start / sr, 'azimuth': None, 'elevation': None})
        
        return traj