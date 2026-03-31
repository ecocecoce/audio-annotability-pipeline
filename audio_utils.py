import librosa
import numpy as np

def load_audio(path, sr=16000):
    y, sr = librosa.load(path, sr=sr)
    return y, sr

def compute_energy_variation(y, frame_length=2048, hop_length=512):

    energy = np.array([
        np.sum(np.abs(y[i:i+frame_length]**2))
        for i in range(0, len(y), hop_length)
    ])
    
    if len(energy) == 0:
        return 0.0
    
    return np.var(energy)

def compute_spectral_flux(y, sr, n_fft=1024, hop_length=512):
    S = np.abs(librosa.stft(y, n_fft=n_fft, hop_length=hop_length))
    flux = np.sqrt(np.sum(np.diff(S, axis=1)**2, axis=0))
    
    if len(flux) == 0:
        return 0.0
    
    return np.mean(flux)