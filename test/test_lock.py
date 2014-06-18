from google.appengine.ext import ndb

import tests

import gaeutils
from gaeutils import locks
from gaeutils import models

class MyModel(models.NoCache):
    val = ndb.IntegerProperty(default=0)

class TestLock(tests.TestBase):
    def setUp(self):
        super(TestLock, self).setUp()

    def test_inc(self):
        lock = locks.Lock.incr('mylock')
        self.assertEqual(lock.ver, 1)
        lock = locks.Lock.incr('mylock')
        self.assertEqual(lock.ver, 2)

        # arbitrary amount
        lock = locks.Lock.incr('mylock', amount=5)
        self.assertEqual(lock.ver, 2+5)

        # negative works
        lock = locks.Lock.incr('mylock', amount=-1)
        self.assertEqual(lock.ver, 2+5-1)

    def test_xaction(self):
        ent = MyModel()
        ent.put()

        def work(fail=False):
            ver = locks.Lock.get('mylock').ver
            entity = ent.key.get()
            entity.val += 2
            entity.put()
            locks.Lock.incr('mylock')
            if fail:
                raise Exception('operation failed')
        ndb.transaction(work, xg=True)
        ent = ent.key.get()
        lock = locks.Lock.get('mylock')
        self.assertEqual(ent.val, 2)
        self.assertEqual(lock.ver, 1)

        try:
            ndb.transaction(lambda: work(fail=True), xg=True)
        except:
            pass
        ent = ent.key.get()
        lock = locks.Lock.get('mylock')
        self.assertEqual(ent.val, 2) # same as after success
        self.assertEqual(lock.ver, 1) # unchanged

    def test_demonstrate_pattern(self):
        """ This just demonstrates typical usage """
        # setup, pretend this was done before
        for i in range(0, 5):
            ent = MyModel(val=i)
            ent.put()

        ## pattern starts here ##    
        # check the ver of particular lock before starting work
        verstart = locks.Lock.get('mylock').ver

        # now pretend like some other process changed the ver
        # after you started working
        locks.Lock.incr('mylock')

        item = MyModel.query(MyModel.val == 3).get()
        def work():
            item.val = 100
            item.put()
            vercheck = locks.Lock.get('mylock').ver
            if vercheck != verstart:
                # stop right here; don't process those items
                # try it again later, or raise an exception, or ...
                raise ndb.Rollback('versioning or sequence issue')
        ndb.transaction(work, retries=0, xg=True)
        ## pattern stops here ##    

        # this just checks that the xaction was rolled back
        for item in MyModel.query():
            # this is true because that work operation
            # never committed
            self.assertTrue(item.val < 100)
