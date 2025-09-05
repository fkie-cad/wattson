import os
import shutil

import psutil


def find_program(candidates: list, fallback: str) -> str:
    """
    Finds a program from the given list of candidates which is installed.
    If no matching program is found, the fallback is returned

    Args:
        candidates (list):
            
        fallback (str):
            
    """
    process = psutil.Process(os.getpid())
    # Check for used shell as parent process
    for p in process.parents():
        if p.name() in candidates:
            program = p.exe()
            return program

    # Search for installed terminals
    for candidate in candidates:
        program = shutil.which(candidate)
        if program is not None:
            return program
    # Return fallback
    return fallback


def get_console_and_shell():
    known_terminals = ["konsole", "gnome-terminal-server", "kitty", "gnome-terminal", "xterm"]
    known_shells = ["bash", "fish", "sh", "ksh", "csh", "tcsh"]
    terminal = find_program(known_terminals, "xterm")
    shell = find_program(known_shells, "/bin/bash")

    if "gnome-terminal-server" in terminal:
        terminal = shutil.which("gnome-terminal")

    return terminal, shell
