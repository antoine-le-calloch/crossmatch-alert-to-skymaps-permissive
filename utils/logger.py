from datetime import datetime, UTC

GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
ENDC = "\033[0m"

def log(message):
    print(f"{datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} - {message}")