import asyncio
import multiprocessing
import time

import numpy as np

print('Oh, i see about ', multiprocessing.cpu_count(), ' cores')


def async_timeit(func):
    async def process(func, *args, **params):
        # Check for coroutine type
        if asyncio.iscoroutinefunction(func):
            print(f'this function is a coroutine: {func.__name__}')
            return await func(*args, **params)
        else:
            print('this is not a coroutine')
            return func(*args, **params)

    async def time_helper(*args, **params):
        print(f'{func.__name__}.time')
        start = time.time()
        result = await process(func, *args, **params)
        stop = time.time()
        print('{!r}  {:2.2f} ms'.format(func.__name__, (stop - start) * 1000))
        return result

    return time_helper


def time_it(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print(
            '%r  %2.2f ms' %
            (method.__name__, (te - ts) * 1000),
        )
        return result

    return timed
