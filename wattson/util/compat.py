def fix_iptc():
    # IP Tables fix for 21.10
    try:
        import iptc
    except Exception:
        import os
        os.environ["XTABLES_LIBDIR"] = "/usr/lib/x86_64-linux-gnu/xtables"