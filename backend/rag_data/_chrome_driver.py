"""Shared Chrome/undetected-chromedriver setup for the Bovada ETL steps.

Both 10_bovada_crawler.py and 11_bovada_scraper.py used to hardcode
driver_executable_path="/usr/local/bin/chromedriver" - the path inside the
Docker image. Anywhere else that file does not exist, so both steps failed
immediately (undetected_chromedriver appends ".exe" on Windows, giving the
confusing "/usr/local/bin/chromedriver.exe" error). Because 10 builds the
games_list.json that 11 consumes, a failure there also silently left 11
scraping whatever stale list was last on disk.
"""
import os
import re
import subprocess

DOCKER_CHROMEDRIVER = "/usr/local/bin/chromedriver"


def local_chrome_major():
    """Major version of the locally installed Chrome, or None.

    undetected_chromedriver otherwise downloads the newest driver, which dies
    with "This version of ChromeDriver only supports Chrome version N" whenever
    the installed browser is a release behind.
    """
    pf = os.environ.get("PROGRAMFILES", r"C:\Program Files")
    pf86 = os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)")
    lad = os.environ.get("LOCALAPPDATA", "")
    candidates = [
        os.path.join(pf, "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(pf86, "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(lad, "Google", "Chrome", "Application", "chrome.exe") if lad else "",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ]
    for exe in candidates:
        if not exe or not os.path.exists(exe):
            continue
        try:
            if os.name == "nt":
                # chrome.exe --version prints nothing on Windows; read metadata.
                out = subprocess.check_output(
                    ["powershell", "-NoProfile", "-Command",
                     "(Get-Item '" + exe + "').VersionInfo.ProductVersion"],
                    text=True, timeout=30,
                )
            else:
                out = subprocess.check_output([exe, "--version"], text=True, timeout=30)
            m = re.search(r"(\d+)\.", out.strip())
            if m:
                return int(m.group(1))
        except Exception:
            continue
    return None


def build_driver(uc, options):
    """Create an undetected_chromedriver Chrome for this environment."""
    if os.path.exists(DOCKER_CHROMEDRIVER):
        return uc.Chrome(options=options,
                         driver_executable_path=DOCKER_CHROMEDRIVER,
                         use_subprocess=False)
    major = local_chrome_major()
    if major:
        print("   Matching chromedriver to local Chrome " + str(major) + ".")
        return uc.Chrome(options=options, version_main=major, use_subprocess=False)
    print("   Chrome version undetected; letting undetected_chromedriver choose.")
    return uc.Chrome(options=options, use_subprocess=False)