import os
import threading
import time
import subprocess
from app import app

def open_in_safari(url: str):
    # give the server a moment to start
    time.sleep(0.8)
    try:
        subprocess.run(["open", "-a", "Safari", url], check=False)
    except Exception:
        pass  # ignore if Safari is not available

if __name__ == "__main__":
    url = f"http://127.0.0.1:5000"
    threading.Thread(target=open_in_safari, args=(url,), daemon=True).start()

    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    app.run(host="127.0.0.1", port=5000, debug=debug)
