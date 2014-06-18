from google.appengine.ext import ndb

from gaeutils import models

class Semaphore(models.NoCache):
    """
    A small datastore entity as a semaphore.
    Its mere presence is a signal.
    """
    name = ndb.StringProperty(required=True)

    @classmethod
    @ndb.transactional
    def get(cls, name):
        key = ndb.Key(cls, name)
        # read by key, eventual consistency does not come into play
        semaphore = key.get()
        if not semaphore:
            semaphore = cls(key=key, name=name)
            semaphore.put()
        return semaphore

class Lock(Semaphore):
    """
    Uses a small datastore entity as a lock.
    A lock is referenced by name, there is a 
    single entity per name.
    It has a version, or sequence, number that can 
    be used to order operations or semantically 
    suggest that the group of controlled data 
    is at a particular version.

    Typical usage pattern:
    1. Get lock to check version number
    2. Do some work
    3. Check lock ver number again
       If it is the same as before, all good.
       If it is different, something changed when 
       you didn't expect it; rollback, schedule later, drop it, etc.
    see tests for example usage
    """
    ver  = ndb.IntegerProperty(default=0)

    @staticmethod
    @ndb.transactional
    def incr(name, amount=1):
        # will propogate a transaction

        # pays the price of double put very first time created
        # but these are typically used frequently and that is better
        # than duplicated code, or a 'create' switch
        lock = Lock.get(name) 
        lock.ver += amount
        lock.put()
        return lock
