import hashlib
import random

from google.appengine.api import memcache
from google.appengine.ext import ndb


DEFAULT_NUM_SHARDS = 10
MAX_NUM_SHARDS = 30
SHARD_KEY_TEMPLATE = 'shard-{}-{:d}'
CACHE_COUNT_KEY = '_counters-{}'
CACHE_LIFE = 60*5


class ShardConfig(ndb.Model):
    """Tracks the number of shards for each named counter."""
    num_shards = ndb.IntegerProperty(default=DEFAULT_NUM_SHARDS)

    @classmethod
    def all_keys(cls, name):
        """Returns all possible keys for the counter name given the config."""
        config = cls.get_or_insert(name)
        shard_keys = [CounterShard.gen_key(name, x) for x in range(config.num_shards)]
        return shard_keys

class CounterShard(ndb.Model):
    """Shards for each named counter."""
    count = ndb.IntegerProperty(default=0)

    @classmethod
    def gen_key(cls, name, index):
        return ndb.Key(CounterShard, SHARD_KEY_TEMPLATE.format(name, index))

    @classmethod
    def gen_random_key(cls, name, num_shards):
        index = random.randint(0, num_shards - 1)
        return cls.gen_key(name, index)


def get_count(name, use_cache=True, cache_life=CACHE_LIFE):
    """Retrieve the value for a given sharded counter."""
    cache_key = CACHE_COUNT_KEY.format(name)
    total = memcache.get(cache_key) if use_cache else None
    if total is None:
        total = 0
        all_keys = ShardConfig.all_keys(name)
        for counter in ndb.get_multi(all_keys):
            if counter is not None:
                total += counter.count
        if use_cache:
            memcache.add(cache_key, total, cache_life)
    return total


def increment(name, delta=1):
    """Increment the value for a given sharded counter."""
    config = ShardConfig.get_or_insert(name)
    _increment(name, config.num_shards, delta=delta)


@ndb.transactional
def _increment(name, num_shards, delta=1):
    """Transactional helper to increment the value for a given sharded counter."""
    key = CounterShard.gen_random_key(name, num_shards)
    counter = key.get()
    if counter is None:
        counter = CounterShard(key=key)
    counter.count += delta
    counter.put()
    # Memcache increment does nothing if the name is not a key in memcache
    memcache.incr(CACHE_COUNT_KEY.format(name), delta=delta)


@ndb.transactional
def increase_shards(name, num_shards):
    """Increase the number of shards for a given sharded counter.

    Will never decrease the number of shards.
    """
    config = ShardConfig.get_or_insert(name)
    if config.num_shards < num_shards and num_shards <= MAX_NUM_SHARDS:
        config.num_shards = num_shards
        config.put()

class Counter(ndb.Model):
    """
    A simple counter.

    A specific counter may have a name, 
    which you bake into the key.

    Counters may also be grouped by domain 
    which allows for a query to get them 
    as a group.
    """
    count  = ndb.IntegerProperty(default=0)
    name   = ndb.StringProperty()
    domain = ndb.StringProperty()

    @ndb.transactional
    def _increment(self, delta=1):
        self.count += delta
        self.put()

    @classmethod
    def gen_key(cls, name, domain=None):
        _key = hashlib.sha224(name.lower() + (domain or '').lower())
        return ndb.Key(cls, _key.hexdigest())

    @classmethod
    @ndb.transactional
    def get(cls, name, domain=None):
        key = cls.gen_key(name, domain=domain)
        return key.get()

    @classmethod
    @ndb.transactional
    def get_or_create(cls, name, domain=None):
        counter = cls.get(name=name, domain=domain)
        if counter is None:
            counter = cls(key=cls.gen_key(name=name, domain=domain), 
                    name=name)
            if domain is not None:
                counter.domain = domain
            counter.put()
        return counter

    @classmethod
    @ndb.transactional
    def increment(cls, name, domain=None, delta=1):
        counter = cls.get_or_create(name, domain=domain)
        counter.count += delta
        counter.put()

    @classmethod
    @ndb.transactional
    def set(cls, name, domain=None, value=0):
        counter = cls.get_or_create(name, domain=domain)
        counter.count = value
        counter.put()
