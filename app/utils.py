"""
Utility functions for time handling and console output.
"""

import sys
from datetime import datetime, timezone, timedelta

def get_target_time_h1():
    """
    Returns the target time for H1 market: Next exact hour.
    """
    now = datetime.now(timezone.utc)
    # Round to next exact hour (e.g., 11:10 -> 12:00)
    target = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return target

def format_time_left(expiry_time):
    """
    Format time remaining until expiry.
    """
    now = datetime.now(timezone.utc)
    time_left = (expiry_time - now).total_seconds()
    
    if time_left < 0:
        return "EXPIRED"
    
    minutes = int(time_left // 60)
    seconds = int(time_left % 60)
    return f"{minutes:02d}:{seconds:02d}"

def clear_screen():
    """
    Clear the entire screen and move cursor to top-left.
    """
    print("\033[2J\033[H", end='')
    sys.stdout.flush()


def kill_process_on_port(port):
    """
    Finds and kills the process running on the specified port.
    """
    import subprocess
    import signal
    import os
    
    try:
        # Find process ID (PID) using lsof
        # -t: terse mode (only PID)
        # -i: select internet files
        cmd = f"lsof -t -i:{port}"
        pid_str = subprocess.check_output(cmd, shell=True).decode().strip()
        
        if pid_str:
            pids = pid_str.split('\n')
            for pid in pids:
                pid = int(pid)
                print(f"⚠️  Порт {port} зайнятий процесом {pid}. Завершуємо...")
                os.kill(pid, signal.SIGTERM)
                # Wait a bit to ensure it's gone
                import time
                time.sleep(1)
    except subprocess.CalledProcessError:
        # No process found on port
        pass
    except Exception as e:
        print(f"❌ Не вдалося звільнити порт {port}: {e}")

