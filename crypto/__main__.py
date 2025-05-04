import requests
import yaml

from datetime import datetime
from pathlib import Path
import threading
import csv
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler 

CONFIG_FILES = iter([
    'config.yml',
    'config.yaml',
])
BASE_URL = "https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit=1"
FIELDNAMES = ['timestamp', 'open', 'high', 'low', 'close', 'volume']

def load_config(config_file: str = "config.yml") -> dict | None:
    try:
        with open(config_file) as stream:
            config = yaml.safe_load(stream)
    except FileNotFoundError:
        print(f"File {config_file} not found.")
        return None
    except yaml.YAMLError as exc:
        print(exc)
        return None

    return config

def crypto_fetch_thread(symbol: str, data_directory: Path) -> None:
    url = BASE_URL.format(symbol=symbol)
    while True:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data:
                open_time, _open, high, low, close, volume, *_ = data[0]
                timestamp = datetime.fromtimestamp(open_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
                with open(data_directory / f"{symbol}.csv", "a", newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
                    writer.writerow({
                        'timestamp': timestamp,
                        'open': _open,
                        'high': high,
                        'low': low,
                        'close': close,
                        'volume': volume
                    })
        else:
            print(f"Failed to fetch data for {symbol}: {response.status_code}")

        time.sleep(60)


def run_http_server(port: int, data_directory: Path) -> None:
    class CustomHTTPRequestHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=data_directory, **kwargs)

    httpd = HTTPServer(('', port), CustomHTTPRequestHandler)
    httpd.serve_forever()


def main():
    try:
        while not (data := load_config(next(CONFIG_FILES))): ...
    except StopIteration:
        print("No config file found.")
        return

    data_directory = Path(data.get("data_directory", "data"))
    data_directory.mkdir(parents=True, exist_ok=True)
    
    threads = []

    for crypto in data.get("cryptos", []):
        symbol = crypto.get("symbol")
        if symbol:
            file = data_directory / f"{symbol}.csv"
            if not file.exists():
                with open(file, "w", newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
                    writer.writeheader()

            thread = threading.Thread(target=crypto_fetch_thread, args=(symbol, data_directory), daemon=True)
            thread.start()
            threads.append(thread)

    http_thread = threading.Thread(target=run_http_server, args=(data.get("port", 8000), data_directory), daemon=True)
    http_thread.start()
    threads.append(http_thread)

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()

