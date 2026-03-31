"""Stage 3: 视频可跟踪性筛选"""

import numpy as np
import cv2
from pathlib import Path
from typing import Dict


class VideoScreener:
    """基于传统 CV 的视频分析"""
    
    def __init__(self, config: dict):
        self.config = config['stage3']
    
    def screen(self, video_path: Path, media_check: Dict) -> Dict:
        result = {
            'video_screening': {
                'num_candidate_instances': 0,
                'track_quality_mean': 0.0,
                'visible_soundable_objects': [],
                'motion_score': 0.0,
                'shot_change_count': 0,
            }
        }
        
        try:
            cap = cv2.VideoCapture(str(video_path))
            if not cap.isOpened():
                return result
            
            fps = cap.get(cv2.CAP_PROP_FPS)
            sample_interval = max(1, int(fps * 0.5))
            
            prev_frame = None
            motion_areas = []
            shot_changes = 0
            frame_count = 0
            
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                
                if frame_count % sample_interval == 0:
                    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                    
                    if prev_frame is not None:
                        diff = cv2.absdiff(gray, prev_frame)
                        _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
                        
                        kernel = np.ones((5, 5), np.uint8)
                        thresh = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel)
                        
                        motion_area = np.sum(thresh > 0)
                        motion_areas.append(motion_area)
                        
                        # 镜头切换检测
                        frame_area = gray.shape[0] * gray.shape[1]
                        if motion_area > frame_area * 0.5:
                            shot_changes += 1
                    
                    prev_frame = gray
                
                frame_count += 1
            
            cap.release()
            
            if motion_areas:
                avg_motion = np.mean(motion_areas)
                max_motion = np.max(motion_areas)
                motion_score = min(1.0, avg_motion / (max_motion + 1e-10))
                num_instances = min(5, max(1, int(avg_motion / 10000)))
                
                result['video_screening'].update({
                    'num_candidate_instances': num_instances,
                    'track_quality_mean': motion_score,
                    'motion_score': float(motion_score),
                    'shot_change_count': shot_changes,
                    'visible_soundable_objects': ['motion_object'] * num_instances,
                })
            
        except Exception as e:
            result['video_screening']['error'] = str(e)[:50]
        
        return result