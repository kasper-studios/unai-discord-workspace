import subprocess
import sys
from pathlib import Path
from workspace import DiscordWorkspace


def register(kernel):
    """Register Discord Workspace in the microkernel."""
    ws = DiscordWorkspace(runtime_id="discord", bus=kernel._bus)
    kernel._manifest_registry["discord"] = {
        "manifest": ws.manifest,
        "capabilities": [],
        "behaviors": [],
        "features": ws.manifest.features,
    }
    return ws


def install(ctx):
    print("Installing Discord Workspace dependencies...")
    req_file = Path(__file__).parent / "requirements.txt"
    if req_file.exists():
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", str(req_file)], check=False)


def start(ctx):
    print("Starting Discord Workspace...")


def stop(ctx):
    print("Stopping Discord Workspace...")


def uninstall(ctx):
    print("Uninstalling Discord Workspace...")
