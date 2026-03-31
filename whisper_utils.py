import whisper

model = whisper.load_model("base")

def compute_speech_ratio(audio_path):
    result = model.transcribe(audio_path, verbose=False)
    
    segments = result.get("segments", [])
    
    if len(segments) == 0:
        return 0.0
    
    total_speech_time = 0.0
    
    for seg in segments:
        start = seg["start"]
        end = seg["end"]
        total_speech_time += (end - start)
    
    total_duration = result["segments"][-1]["end"]
    
    if total_duration == 0:
        return 0.0
    
    return total_speech_time / total_duration