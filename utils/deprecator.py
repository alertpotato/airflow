import functools
import warnings


LOGS_WARNING_TEXT = ("Теперь всё взаимодействие с логами (meta)"
                     " должно быть через utils.logs_meta.*")


def deprecated(reason: str):
    """
    Этот декоратор используется для функций, от которых нужно
    отказаться как можно скорее.

    'Although never is often better than *right* now.'
    """
    def decorator(fn):
        @functools.wraps(fn)
        def new_f(*args, **kwargs):
            msg = f"Функция {fn.__name__} отмечена как устаревшая!"
            if reason:
                msg += f" Причина: {reason}"
            warnings.simplefilter('always', DeprecationWarning)
            warnings.warn(
                msg,
                category=DeprecationWarning,
                stacklevel=2
            )
            warnings.simplefilter('default', DeprecationWarning)
            return fn(*args, **kwargs)
        return new_f
    return decorator
