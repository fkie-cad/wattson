def set_thread_name(name: str):
    try:
        import pyprctl
        pyprctl.set_name(name)
    except Exception:
        pass
