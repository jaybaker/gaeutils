
from google.appengine.ext import ndb

class NoCache(ndb.Model):
    """
    No cache mixin. Uses ndb cache policy.
    """
    _use_cache    = False
    _use_memcache = False

class FauxFuture(object):
    """
    Stand in when not really querying
    """
    def __init__(self, data=None):
        self.data = data

    def get_result(self):
        return self.data

    def __repr__(self):
        return self.__class__.__name__
