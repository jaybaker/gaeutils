from google.appengine.ext import ndb

import tests

import gaeutils
from gaeutils import counters

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
