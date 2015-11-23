import tests

from google.appengine.ext import ndb
from google.appengine.api import memcache

from gaeutils import models
from gaeutils import Stack, FutStack

class NoCacheModel(tests.TestModel, models.NoCache):
    pass

class TestNDBCache(tests.TestBase):
    def setUp(self):
        super(TestNDBCache, self).setUp()
        ctx = ndb.get_context()
        self.ndb_cache_pre = ctx._memcache_prefix

        self.cache_me_key = ndb.Key(tests.TestModel, 'cached')
        self.cache_me = tests.TestModel(key=self.cache_me_key)
        self.cache_me.put()

        self.no_cache_me_key = ndb.Key(NoCacheModel, 'not_cached')
        self.no_cache_me = NoCacheModel(key=self.no_cache_me_key)
        self.no_cache_me.put()


    def mkey(self, key):
        return self.ndb_cache_pre + key.urlsafe()

    @ndb.toplevel
    def testNDBMemcache(self):
        # prime ndb cache
        entity = self.cache_me_key.get()
        entity = self.no_cache_me_key.get()

        # test that canonical ndb memcache is working
        ctx = ndb.get_context()
        self.assertTrue(ctx._use_memcache(self.cache_me_key))
        self.assertTrue(memcache.get(self.mkey(self.cache_me_key)) is not None)

        # the model with no cache should not be in memcache
        self.assertTrue(memcache.get(self.mkey(self.no_cache_me_key)) is None)


class StackTest(tests.TestBase):
    def test_simple_case(self):
        s = Stack().push('a').push('b')
        self.assertEqual('b', s.pop())
        self.assertEqual('a', s.pop())

    def test_with_context(self):
        # no error
        with Stack() as s:
            s.push('a').push('b')
            s.pop()
            s.pop()

        # forgot to pop something
        try:
            with Stack() as s:
                s.push('a').push('b')
                s.pop()
            self.fail()
        except AssertionError:
            pass

    def test_future_stack(self):
        s = FutStack()
        s.push(models.FauxFuture(data='a'))
        s.push(models.FauxFuture(data='b'))
        self.assertEqual('b', s.pop())
        self.assertEqual('a', s.pop())
