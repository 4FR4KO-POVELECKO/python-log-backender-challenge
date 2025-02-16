# flake8: noqa
import functools
import sentry_sdk


def sentry_transaction(name: str):
    def decorator(func): 
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with sentry_sdk.start_transaction(op="task", name=name):
                return func(*args, **kwargs)
        return wrapper
    return decorator
