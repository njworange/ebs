import queue
import threading

from .models import ModelEbsEpisode


class QueueService:
    download_queue = None
    download_thread = None
    queued_ids = set()
    queue_lock = threading.Lock()

    @classmethod
    def ensure_queue(cls) -> None:
        if cls.download_queue is None:
            cls.download_queue = queue.Queue()

    @classmethod
    def enqueue_item(cls, item_id: int) -> bool:
        cls.ensure_queue()
        q = cls.download_queue
        if q is None:
            return False
        with cls.queue_lock:
            if item_id in cls.queued_ids:
                return False
            cls.queued_ids.add(item_id)
            q.put(item_id)
        return True

    @classmethod
    def current_items(cls) -> list[dict]:
        with cls.queue_lock:
            queued_id_list = sorted(cls.queued_ids)
        items = []
        for qid in queued_id_list:
            item = ModelEbsEpisode.get_by_id(qid)
            if item:
                items.append(item.as_dict())
        return items

    @classmethod
    def finish_item(cls, item_id: int) -> None:
        with cls.queue_lock:
            cls.queued_ids.discard(item_id)
        if cls.download_queue is not None:
            cls.download_queue.task_done()

    @classmethod
    def reset(cls) -> int:
        with cls.queue_lock:
            count = len(cls.queued_ids)
            cls.queued_ids.clear()
            if cls.download_queue is not None:
                cls.download_queue.queue.clear()
        return count
