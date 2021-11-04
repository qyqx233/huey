import os
import redis

if os.environ.get('WORKER_CLASS') in ('greenlet', 'gevent'):
    print('Monkey-patching for gevent.')
    from gevent import monkey

    monkey.patch_all()
import sys

from config import huey
from tasks import add

if __name__ == '__main__':
    result = add(1, 2)
    print(result.__class__)
