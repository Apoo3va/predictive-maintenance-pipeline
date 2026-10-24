"""
Simulated accelerometer + DSP feature extraction.

Models a rotating machine: shaft-frequency vibration, optional bearing-fault
impulses, Gaussian sensor noise, and 12-bit ADC quantization.
"""
import numpy as np

FS = 2000            # sampling rate in Hz (Nyquist = 1000 Hz)
ADC_BITS = 12
ADC_RANGE_G = 4.0    # +/- 4 g full scale


def simulate_vibration(rpm, fault=False, duration=1.0, fs=FS, rng=None):
    """Return one window of accelerometer samples (in g) after ADC quantization."""
    rng = rng or np.random.default_rng()
    n = int(fs * duration)
    t = np.arange(n) / fs
    f_shaft = rpm / 60.0

    # Healthy machine: shaft rotation and its 2nd harmonic
    x = 0.5 * np.sin(2 * np.pi * f_shaft * t) + 0.2 * np.sin(2 * np.pi * 2 * f_shaft * t)

    if fault:
        # Bearing fault: periodic impacts that excite a 500 Hz structural resonance
        f_fault = 3.58 * f_shaft
        tau = np.arange(int(0.02 * fs)) / fs
        burst = np.exp(-tau / 0.004) * np.sin(2 * np.pi * 500 * tau)
        for start in np.arange(0, duration, 1.0 / f_fault):
            i = int(start * fs)
            end = min(i + len(burst), n)
            x[i:end] += 1.5 * burst[: end - i]

    # Sensor noise
    x += rng.normal(0, 0.1, n)

    # ADC quantization: clip to range, round to the nearest LSB
    lsb = (2 * ADC_RANGE_G) / (2 ** ADC_BITS)
    x = np.clip(x, -ADC_RANGE_G, ADC_RANGE_G)
    return np.round(x / lsb) * lsb


def extract_features(x, fs=FS):
    """Time-domain and frequency-domain features for one window."""
    x = x - np.mean(x)
    rms = float(np.sqrt(np.mean(x ** 2)))
    peak = float(np.max(np.abs(x)))
    crest = peak / rms
    kurtosis = float(np.mean(x ** 4) / (np.var(x) ** 2))

    # FFT with a Hann window to reduce spectral leakage
    spectrum = np.abs(np.fft.rfft(x * np.hanning(len(x)))) / len(x)
    freqs = np.fft.rfftfreq(len(x), 1.0 / fs)
    dominant_hz = float(freqs[1:][np.argmax(spectrum[1:])])  # skip DC bin

    # Share of spectral energy in the 300-800 Hz band, where the fault resonance sits
    band = (freqs >= 300) & (freqs <= 800)
    band_energy = float(np.sum(spectrum[band] ** 2) / np.sum(spectrum ** 2))

    return {
        "rms": rms,
        "peak": peak,
        "crest_factor": crest,
        "kurtosis": kurtosis,
        "dominant_hz": dominant_hz,
        "band_energy": band_energy,
    }


if __name__ == "__main__":
    rng = np.random.default_rng(42)
    rpm = 1800  # shaft frequency = 30 Hz
    for label, fault in (("healthy", False), ("faulty", True)):
        feats = extract_features(simulate_vibration(rpm, fault=fault, rng=rng))
        print(label, {k: round(v, 3) for k, v in feats.items()})