"""Stage 6: 路由决策"""

from typing import Dict


class Router:
    """综合所有指标，决定 clip 进入哪个池"""
    
    def __init__(self, config: dict):
        self.config = config['routing']
    
    def route(self, all_results: Dict) -> Dict:
        routing = {'tier': 'rejected', 'reasons': []}
        
        media = all_results.get('media_check', {})
        audio = all_results.get('audio_screening', {})
        video = all_results.get('video_screening', {})
        av = all_results.get('av_screening', {})
        spatial = all_results.get('spatial_screening', {})
        
        # 基础检查
        if not media.get('decode_ok', False):
            routing['reasons'].append('Media decode failed')
            return {'routing': routing}
        
        # 高质量池检查
        checks = {
            'audio_event_density': audio.get('event_density', 0) >= 0.3,
            'voiceover_risk': audio.get('voiceover_risk', 1) <= 0.5,
            'visible_motion': video.get('motion_score', 0) >= 0.3,
            'shot_changes': video.get('shot_change_count', 999) <= 3,
            'av_correlation': av.get('av_correlation', 0) >= 0.2,
            'spatial_validity': spatial.get('spatial_validity_score', 0) >= 0.5,
        }
        
        if all(checks.values()):
            routing['tier'] = 'high_quality'
            routing['checks_passed'] = checks
        elif media.get('decode_ok', False):
            routing['tier'] = 'weak_quality'
            routing['failed_checks'] = [k for k, v in checks.items() if not v]
        else:
            routing['reasons'].append('Media not OK')
        
        return {'routing': routing}