import os
from pathlib import Path
import site
import subprocess
import sys
import venv

VENV_DIR = os.getenv("X9_FETCHER_VENV_DIR", "x9_data_fetcher/executor")

REQUIRED_PACKAGES = [
    "python-dotenv",
    "pandas",
    "pyarrow",
    "websockets",
]

REQUIREMENTS_FILE = Path("x9_data_fetcher/requirements.txt")


def create_and_activate_venv() -> bool:
    """
    Create venv if missing and inject its site-packages into current process.
    """
    if not os.path.exists(VENV_DIR):
        print(f"[X9_DEPTH] Creating virtual environment: {VENV_DIR}", flush=True)
        venv.create(VENV_DIR, with_pip=True)

        pip_path = os.path.join(VENV_DIR, "Scripts" if os.name == "nt" else "bin", "pip")
        subprocess.check_call([pip_path, "install", "--upgrade", "pip"])
        if REQUIREMENTS_FILE.exists():
            subprocess.check_call([pip_path, "install", "-r", str(REQUIREMENTS_FILE)])
        else:
            subprocess.check_call([pip_path, "install", *REQUIRED_PACKAGES])

    site_packages = os.path.join(
        VENV_DIR,
        "Lib" if os.name == "nt" else "lib",
        f"python{sys.version_info.major}.{sys.version_info.minor}",
        "site-packages",
    )
    if site_packages not in sys.path:
        site.addsitedir(site_packages)

    return True
