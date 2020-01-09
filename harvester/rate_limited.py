import time
import threading
from functools import wraps

def rate_limited(max_per_second):
    """Rate-limits the decorated function"""
    lock = threading.Lock()
    min_interval = 1.0 / max_per_second
    try:
        preferred_clock = time.perf_counter # Python 3.4 or above
    except AttributeError:
        preferred_clock = time.clock # Earlier than Python 3.8

    def decorate(func):
        last_time_called = preferred_clock()

        @wraps(func)
        def rate_limited_function(*args, **kwargs):
            lock.acquire()
            nonlocal last_time_called
            try:
                elapsed = preferred_clock() - last_time_called
                left_to_wait = min_interval - elapsed

                if left_to_wait > 0:
                    time.sleep(left_to_wait)

                return func(*args, **kwargs)
            finally:
                last_time_called = preferred_clock()
                lock.release()

        return rate_limited_function

    return decorate
