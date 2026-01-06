import time
from pathlib import Path
from watchdog.observers import Observer

from quicksql.core.watcher import QsqlFileEventHandler


def watch_file(file_path: str):
    target_file = Path(file_path).resolve()
    watch_dir = target_file.parent

    event_handler = QsqlFileEventHandler(target_file)
    observer = Observer()
    observer.schedule(event_handler, str(watch_dir), recursive=False)
    observer.start()

    print(f"Watching {target_file}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":
    watch_file("base.sql")
