
from google.appengine.ext import ndb

class NoCache(ndb.Model):
    """
    No cache mixin. Uses ndb cache policy.
    """
    _use_cache    = False
    _use_memcache = False
