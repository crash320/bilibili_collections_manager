import logging
import yaml
import os
import time
from functools import wraps
from queue import Queue
from threading import Thread

def load_config():
    with open('config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def setup_logging(config):
    logging.basicConfig(
        level=getattr(logging, config['logging']['level']),
        format=config['logging']['format'],
        handlers=[
            logging.FileHandler(config['logging']['file']),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('BilibiliCollector')

def retry_on_failure(max_retries=3, delay=5):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == max_retries - 1:
                        raise e
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

class DownloadQueue:
    def __init__(self, max_workers=2):
        self.queue = Queue()
        self.max_workers = max_workers
        self.workers = []
        self._start_workers()

    def _start_workers(self):
        for _ in range(self.max_workers):
            worker = Thread(target=self._worker, daemon=True)
            worker.start()
            self.workers.append(worker)

    def _worker(self):
        while True:
            task = self.queue.get()
            if task is None:
                break
            try:
                task()
            except Exception as e:
                logging.error(f"Download task failed: {e}")
            finally:
                self.queue.task_done()

    def add_task(self, task):
        self.queue.put(task)

    def stop(self):
        for _ in self.workers:
            self.queue.put(None)
        for worker in self.workers:
            worker.join() 