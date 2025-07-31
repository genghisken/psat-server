#!/usr/bin/env python3
"""
Image Shift Assessor.

Usage:
  assess_shift.py <ref_fits> <target_fits> [--profile=<p>] [--prominence=<s>] [--shiftcut=<d>]
                  [--sigma=<g>] [--fitwin=<w>] [--nan=<policy>] [--resid=<mode>] [--plot]

Options:
  <ref_fits>              Path to the reference FITS file.
  <target_fits>           Path to the target FITS file.
  --profile=<p>           Peak profile model [default: moffat].
  --prominence=<s>        SNR threshold for prominence [default: 6.0].
  --shiftcut=<d>          Pixel shift threshold [default: 10.0].
  --sigma=<g>             Gaussian filter sigma [default: 1.0].
  --fitwin=<w>            Fitting window size [default: 20].
  --nan=<policy>          NaN handling policy (median-fill or mask) [default: median-fill].
  --resid=<mode>          Residual plot mode (map, hist, heat) [default: map].
  --plot                  Show diagnostic plots.
"""

from docopt import docopt
import numpy as np
import matplotlib.pyplot as plt
from astropy.io import fits
from astropy.visualization import ZScaleInterval
from scipy.fft import fft2, ifft2, fftshift
from scipy.ndimage import gaussian_filter
from scipy.optimize import curve_fit, OptimizeWarning
from gkutils.commonutils import Struct, cleanOptions
import warnings

# ======================================
# 1) I/O & pre-processing helpers
# ======================================
def load_fits(path):
    """Return (float32 array, header) from the first HDU that has data."""
    with fits.open(path, memmap=False) as hdul:
        for hdu in hdul:
            if hdu.data is not None:
                return hdu.data.astype(np.float32), hdu.header
    raise ValueError(f"No image data in {path}")

def zscale_image(data):
    vmin, vmax = ZScaleInterval().get_limits(data)
    clipped = np.clip(data, vmin, vmax)
    return np.where(vmax > vmin,
                    (clipped - vmin) / (vmax - vmin),
                    np.zeros_like(clipped, dtype=np.float32))

# ======================================
# 2) Peak-shape primitives
# ======================================
def _gauss2d(c, A, x0, y0, sx, sy, off):
    x, y = c
    return A * np.exp(-(((x-x0)**2)/(2*sx**2) + ((y-y0)**2)/(2*sy**2))) + off

def _moffat2d(c, A, x0, y0, ax, ay, beta, off):
    x, y = c
    return A * (1 + ((x-x0)**2)/ax**2 + ((y-y0)**2)/ay**2)**(-beta) + off

def _gauss1d(x, A, mu, sig, off):
    return A * np.exp(-0.5 * ((x-mu)/sig)**2) + off

def _moffat1d(x, A, mu, alpha, beta, off):
    return A * (1 + ((x-mu)/alpha)**2)**(-beta) + off

# ======================================
# 3) QC function with robust fallback
# ======================================
def assess_shift(img_ref, img_new, *,
                 profile="gaussian",
                 prominence_cut=5.0,
                 shift_cut=1.0,
                 smooth_sigma=1.5,
                 fit_window=9,
                 nan_policy="median-fill",
                 residual_mode="map",   # "map", "hist", or "heat"
                 make_plots=False):

    # --- NaN handling --------------------------------------------------
    if nan_policy == "median-fill":
        img_ref = np.nan_to_num(img_ref, nan=np.nanmedian(img_ref))
        img_new = np.nan_to_num(img_new, nan=np.nanmedian(img_new))
    elif nan_policy == "mask":
        mask = np.isnan(img_ref) | np.isnan(img_new)
        img_ref = np.where(mask, 0, img_ref)
        img_new = np.where(mask, 0, img_new)
    else:
        raise ValueError("nan_policy must be 'median-fill' or 'mask'")

    # --- Phase-correlation surface ------------------------------------
    eps  = 1e-12
    cps  = fft2(img_ref) * np.conj(fft2(img_new))
    cps /= (np.abs(cps) + eps)
    corr = fftshift(np.abs(ifft2(cps)))
    corr_s = gaussian_filter(corr, smooth_sigma)

    # --- Edge-margin check: discard if peak is too close to border -----
    py, px = np.unravel_index(np.argmax(corr_s), corr_s.shape)
    H, W   = corr_s.shape
    margin = 10  # pixels
    if (px < margin) or (px >= W - margin) or (py < margin) or (py >= H - margin):
        return {
            'shift': (float('nan'), float('nan')),
            'snr': 0.0,
            'significant': False,
            'fit_used': False,
            'profile': profile
        }

    # --- Locate integer peak & set up subwindow -----------------------
    half = fit_window // 2

    # clamp to image boundaries
    y0 = max(0,      py - half)
    y1 = min(H,      py + half + 1)
    x0 = max(0,      px - half)
    x1 = min(W,      px + half + 1)

    win = corr_s[y0:y1, x0:x1]
    if win.size == 0:
        raise RuntimeError(
            f"Empty fit window: py,px=({py},{px}), window=([{y0}:{y1}], [{x0}:{x1}])"
        )

    yy, xx = np.mgrid[y0:y1, x0:x1]  # global coords

    # Pick model & starting guesses
    if profile == "gaussian":
        model2d = _gauss2d
        p0 = (win.max(), px, py, 1.0, 1.0, np.median(corr_s))
        one_d = lambda xs, p: _gauss1d(xs, p[0], p[1], p[3], p[5])
    elif profile == "moffat":
        model2d = _moffat2d
        p0 = (win.max(), px, py, 1.0, 1.0, 4.5, np.median(corr_s))
        one_d = lambda xs, p: _moffat1d(xs, p[0], p[1], p[3], p[5], p[-1])
    else:
        raise ValueError("profile must be 'gaussian' or 'moffat'")

    # --- Robust non-linear fit ----------------------------------------
    def model1d(c, *pars):
        return model2d(c, *pars).ravel()

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", OptimizeWarning)
            popt, _ = curve_fit(model1d, (xx, yy), win.ravel(),
                                p0=p0, maxfev=20_000)
        amp, cx, cy = popt[0], popt[1], popt[2]
        off = popt[-1]
        used_fit = True
    except RuntimeError:
        # fallback: no fit, just integer peak
        cx, cy = px, py
        amp    = win.max()
        off    = np.median(corr_s)
        used_fit = False

    # --- SNR estimate --------------------------------------------------
    ann = corr_s.copy()
    ann[y0:y1, x0:x1] = np.nan
    noise_sig = np.nanmedian(np.abs(ann - np.nanmedian(ann))) * 1.4826
    snr = (amp - off) / (noise_sig + eps)

    # --- Shift relative to centre -------------------------------------
    dy, dx = cy - H//2, cx - W//2
    if dy >  H/2: dy -= H
    if dx >  W/2: dx -= W
    mag = np.hypot(dy, dx)
    significant = (snr >= prominence_cut) and (mag > shift_cut)

    # --- Optional diagnostics (unchanged) -----------------------------
    if make_plots:
        import matplotlib.gridspec as gridspec
        from matplotlib.ticker import MaxNLocator

        fig = plt.figure(figsize=(18, 11))
        gs  = gridspec.GridSpec(3, 3, height_ratios=[1,1,0.55],
                                hspace=0.4, wspace=0.25)

        ax00 = fig.add_subplot(gs[0,0])
        ax00.imshow(img_ref, origin='lower', cmap='gray')
        ax00.set_title('Reference (Z-scaled)')
        ax00.axis('off')

        ax01 = fig.add_subplot(gs[0,1])
        ax01.imshow(img_new, origin='lower', cmap='gray')
        ax01.set_title('Target (Z-scaled)')
        ax01.axis('off')

        ax02 = fig.add_subplot(gs[0,2])
        ext = (-W//2, W//2-1, -H//2, H//2-1)
        im_corr = ax02.imshow(corr_s, origin='lower', cmap='viridis', extent=ext)
        ax02.axhline(0, ls='--', lw=0.7, c='0.75')
        ax02.axvline(0, ls='--', lw=0.7, c='0.75')
        ax02.scatter(dx, dy, c='red', s=60, marker='x', label='peak')
        ax02.set_xlabel('Δx  (pixels)')
        ax02.set_ylabel('Δy  (pixels)')
        ax02.set_title('Smoothed phase-corr (0,0 at centre)')
        ax02.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax02.yaxis.set_major_locator(MaxNLocator(integer=True))
        ax02.legend(fontsize=7, loc='upper right')
        plt.colorbar(im_corr, ax=ax02, fraction=0.048)

        ax10 = fig.add_subplot(gs[1,0])
        ax10.imshow(win, origin='lower', cmap='plasma')
        ax10.set_title('Fit window (pixels)')
        ax10.axvline(half, ls=':', lw=1, color='white')
        ax10.axhline(half, ls=':', lw=1, color='white')

        ax11 = fig.add_subplot(gs[1,1])
        if used_fit:
            fit_patch = model2d((xx, yy), *popt).reshape(win.shape)
            res_img = win - fit_patch
        else:
            res_img = win - win.mean()

        if residual_mode.lower() == "hist":
            ax11.hist(res_img.ravel(), bins=30)
            ax11.set_xlabel('Residual value')
            ax11.set_ylabel('Count')
            ax11.set_title('Residual histogram')
        elif residual_mode.lower() in ("map", "heat"):
            im_res = ax11.imshow(res_img, origin='lower', cmap='coolwarm')
            ax11.set_title('Residual map')
            plt.colorbar(im_res, ax=ax11, fraction=0.048)
        else:
            raise ValueError("residual_mode must be 'map', 'hist', or 'heat'")

        ax12 = fig.add_subplot(gs[1,2])
        xs = np.arange(x0, x1)
        mid = win[half]
        ax12.plot(xs, mid, 'k.', label='data')
        if used_fit:
            ax12.plot(xs, one_d(xs, popt), 'r-', label='fit')
        ax12.axhline(off + noise_sig * prominence_cut,
                     ls='--', lw=1, label=f'{prominence_cut} σ cut')
        ax12.set_title('Central row through peak')
        ax12.legend(fontsize=8)

        ax20 = fig.add_subplot(gs[2,:])
        ax20.axis('off')
        verdict = "REJECT" if significant else "KEEP"
        info = (f"SNR               : {snr:.2f}\n"
                f"SNR threshold     : {prominence_cut:.2f}\n"
                f"Shift |Δ| (pixels): {mag:.2f}\n"
                f"Shift threshold   : {shift_cut:.2f}\n\n"
                f"Decision          : {verdict}")
        ax20.text(0.01, 0.98, info, va='top', ha='left',
                  fontsize=13, family='monospace',
                  bbox=dict(facecolor='#f6f6f6', edgecolor='0.6'))

        fig.suptitle(f"Peak (dy,dx)=({dy:.2f}, {dx:.2f}) px  •  "
                     f"SNR={snr:.2f}  •  {verdict}",
                     fontsize=15, y=0.995)
        plt.show()

    return {
        'shift': (float(dy), float(dx)),
        'snr': float(snr),
        'significant': bool(significant),
        'fit_used': bool(used_fit),
        'profile': profile
    }


def pixelshiftCheck(options):

    ref_path   = options.ref_fits
    tgt_path   = options.target_fits
    profile    = options.profile
    prominence = float(options.prominence)
    shiftcut   = float(options.shiftcut)
    sigma      = float(options.sigma)
    fitwin     = int(options.fitwin)
    nan_policy = options.nan
    resid_mode = options.resid
    plot       = options.plot

    ref_raw, _ = load_fits(ref_path)
    new_raw, _ = load_fits(tgt_path)

    ref = zscale_image(ref_raw)
    new = zscale_image(new_raw)

    result = assess_shift(
        ref, new,
        profile=profile,
        prominence_cut=prominence,
        shift_cut=shiftcut,
        smooth_sigma=sigma,
        fit_window=fitwin,
        nan_policy=nan_policy,
        residual_mode=resid_mode,
        make_plots=plot
    )

    print(ref_path)
    print("\n--- Shift Result ---")
    for key, val in result.items():
        print(f"{key}: {val}")



def main():

    args = docopt(__doc__)

    opts = cleanOptions(args)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)

    pixelshiftCheck(options)

if __name__ == "__main__":
    main()
