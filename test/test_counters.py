from google.appengine.ext import ndb

import tests

import gaeutils
from gaeutils import counters
from gaeutils.counters import Counter

class TestCounters(tests.TestBase):
    def setUp(self):
        super(TestCounters, self).setUp()

    def test_increment(self):
        """Happy path """
        counters.increment('testcounter')
        self.assertEqual(counters.get_count('testcounter'), 1)
        counters.increment('testcounter', delta=5)
        self.assertEqual(counters.get_count('testcounter'), 6)

    def test_read_before_write(self):
        self.assertEqual(counters.get_count('neverbeenseen'), 0)

    def test_shard(self):
        """Tests internal counter structure. """
        name = 'testcounter'
        shards = ndb.get_multi(counters.ShardConfig.all_keys(name))
        counters.increment(name) 
        config = counters.ShardConfig.get_by_id(name)
        self.assertTrue(config is not None)
        shards = ndb.get_multi(counters.ShardConfig.all_keys(name))
        self.assertTrue(len(shards) > 1) # this will be gt 1 but all but 1 are None
        shards = filter(None, shards)
        self.assertEqual(len(shards), 1) # there should be only 1 shard since only one increment

    def test_many_increment(self):
        name = 'testcounter'
        for i in range(0, 100):
            counters.increment(name) 
        self.assertEqual(counters.get_count(name), 100)

class TestSimpleCounter(tests.TestBase):
    def test_incr_none_existing(self):
        Counter.increment(name='foo')
        self.assertEqual(1, len(Counter.query().fetch()))
        counter = Counter.get_or_create(name='foo')
        self.assertEqual(1, counter.count)

    def test_increment_existing(self):
        Counter.increment(name='foo')
        Counter.increment(name='foo')
        self.assertEqual(1, len(Counter.query().fetch()))
        counter = Counter.get_or_create(name='foo')
        self.assertEqual(2, counter.count)

    def test_domain(self):
        # this one does NOT belong to a domain
        Counter.increment(name='foo')
        Counter.increment(name='foo', domain='zzz')
        Counter.increment(name='foo', domain='zzz')
        # this one is in the same domain as previous counter
        Counter.increment(name='bar', domain='zzz')
        counter1 = Counter.get_or_create(name='foo')
        counter2 = Counter.get_or_create(name='foo', domain='zzz')
        self.assertNotEqual(counter1.count, counter2.count)
        counters = Counter.query(Counter.domain == 'zzz').fetch()
        self.assertEqual(2, len(counters))
