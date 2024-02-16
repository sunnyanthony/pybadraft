from functools import wraps

def synchronizer(lock_attr_name):
    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            lock = getattr(self, lock_attr_name)
            with lock:
                return method(self, *args, **kwargs)
        return wrapper
    return decorator