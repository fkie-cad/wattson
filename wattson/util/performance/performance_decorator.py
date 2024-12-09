import time


def performance_assert(warning_threshold: float = 0.5):
    def performance_decorator(fct):
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = fct(*args, **kwargs)
            duration = time.perf_counter() - start
            if duration >= warning_threshold:
                print(f"[PERFORMANCE WARNING] {fct.__name__} took {duration}s")
                for arg in args:
                    print(repr(arg))
                for kwarg, value in kwargs.items():
                    print(f"{kwarg} = {repr(value)}")
            return result

        return wrapper
    return performance_decorator
