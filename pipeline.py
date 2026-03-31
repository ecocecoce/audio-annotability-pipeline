"""Pipeline 组装"""

from pathlib import Path
from typing import Dict, Optional

from config import CONFIG
from stage1_media import MediaChecker
from stage2_audio import AudioScreener
from stage3_video import VideoScreener
from stage4_av import AVScreener
from stage5_spatial import SpatialScreener
from stage6_router import Router


class CleaningPipeline:
    """完整的数据清洗 Pipeline"""
    
    def __init__(self, config: dict = None):
        self.config = config or CONFIG
        
        self.checker = MediaChecker(self.config)
        self.audio_screener = AudioScreener(self.config)
        self.video_screener = VideoScreener(self.config)
        self.av_screener = AVScreener(self.config)
        self.spatial_screener = SpatialScreener(self.config)
        self.router = Router(self.config)
    
    def process(self, video_path: Path, external_audio_path: Optional[Path] = None) -> Dict:
        """
        处理单个视频，可选外部分离的4通道WAV音频
        
        Args:
            video_path: 视频文件路径
            external_audio_path: 可选的外部4通道WAV路径
        """
        
        # Stage 1: 媒体检查（支持外部音频）
        result = self.checker.check(video_path, external_audio_path)
        
        # 如果媒体检查失败，直接路由
        if not result['media_check']['decode_ok']:
            result.update(self.router.route(result))
            return result
        
        # Stage 2: 音频分析（优先外部WAV）
        if external_audio_path and result['media_check'].get('has_external_foa'):
            audio_result = self.audio_screener.screen_wav(external_audio_path)
            spatial_result = self.spatial_screener.screen_wav(external_audio_path)
        else:
            # 回退到视频内音频
            audio_result = self.audio_screener.screen(video_path, result['media_check'])
            spatial_result = self.spatial_screener.screen(video_path, result['media_check'])
        
        # Stage 3: 视频分析（始终用视频文件）
        video_result = self.video_screener.screen(video_path, result['media_check'])
        
        # Stage 4: AV分析
        av_result = self.av_screener.screen(audio_result, video_result)
        
        # 合并所有结果
        all_results = {
            **result,
            **audio_result,
            **video_result,
            **av_result,
            **spatial_result,
        }
        
        # Stage 6: 路由决策
        routing = self.router.route(all_results)
        all_results.update(routing)
        
        return all_results