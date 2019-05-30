import os
import json
import logging

logger = logging.getLogger(__name__)


class FileCacher(object):

    def __init__(self, should_cache_files=False):
        self._should_cache_files = should_cache_files
        self._cache_path = os.path.dirname(os.path.abspath(__file__)) + os.sep + 'cache'

    def write_cache(self, key, data):
        if (self._should_cache_files):
            path = self._cache_path + os.sep + key
            json.dump(data, open(path, 'w'))

    def read_cache(self, key):
        path = self._cache_path + os.sep + key
        if (self._should_cache_files):
            if os.path.isfile(path):
                logger.debug('Reading cache for ' + key)
                return json.load(open(path))
            else:
                return None
        else:
            return None
