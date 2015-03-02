import tests

from google.appengine.api import memcache

class TestMemcacheNoHook(tests.TestBase):
    """
    A baseline. A few simple memcache tests to compare 
    with the same tests with the hook in place.
    """
    def setUp(self):
        super(TestMemcacheNoHook, self).setUp()
        memcache.set('foo', 'bar')

    def testSet(self):
        pass

    def testSetGet(self):
        self.assertEqual(memcache.get('foo'), 'bar')

class TestMemcacheHook(tests.TestBase):
    def setUp(self):
        super(TestMemcacheHook, self).setUp()
        from gaeutils import caching

        caching.memcache_hook(use_cache=False)
        memcache.set('foo', 'bar')

    def testSet(self):
        pass

    def testSetGet(self):
        self.assertTrue(memcache.get('foo') is None)

    def testAdd(self):
        # Just checking that other verb in the api
        memcache.add('foobar', 'bar')
        self.assertTrue(memcache.get('foobar') is None)

    def testNamespace(self):
        memcache.set('foobar', 'bar', namespace='xyz')
        self.assertTrue(memcache.get('foobar', namespace='xyz') is None)
