"""Stage 4: 声画一致性粗筛"""

from pathlib import Path
from typing import Dict


class AVScreener:
    """基于相关性的声画分析"""
    
    def __init__(self, config: dict):
        self.config = config['stage4']
    
    def screen(self, audio_screening: Dict, video_screening: Dict) -> Dict:
        """分析声画一致性（接受预计算的音频/视频结果）"""
        result = {
            'av_screening': {
                'max_soundingness': 0.0,
                'soundingness_entropy': 1.0,
                'av_correlation': 0.0,
            }
        }
        
        try:
            audio_density = audio_screening.get('event_density', 0)
            motion_score = video_screening.get('motion_score', 0)
            
            # 声画相关性
            correlation = 1.0 - abs(audio_density - motion_score)
            soundingness = max(0, correlation)
            
            result['av_screening'].update({
                'max_soundingness': float(soundingness),
                'soundingness_entropy': 0.5 if soundingness > 0.3 else 0.8,
                'av_correlation': float(correlation),
            })
            
        except Exception as e:
            result['av_screening']['error'] = str(e)[:50]
        
        return result