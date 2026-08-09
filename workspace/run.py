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
    print("Installing Discord Workspace...")


def start(ctx):
    print("Starting Discord Workspace...")


def stop(ctx):
    print("Stopping Discord Workspace...")


def uninstall(ctx):
    print("Uninstalling Discord Workspace...")
