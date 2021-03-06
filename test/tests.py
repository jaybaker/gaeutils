import unittest
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util

import gaeutils

class TestModel(ndb.Model):
    """A model class used for testing."""
    number = ndb.IntegerProperty(default=42)
    text   = ndb.StringProperty()

class TestBase(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()

        self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
        self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)
        self.testbed.init_memcache_stub()
        self.testbed.init_app_identity_stub()
        #self.testbed.init_taskqueue_stub(root_path=os.path.join('.'))
        self.testbed.init_taskqueue_stub()

        gaeutils.App.setup() # needed in test env

    def tearDown(self):
        self.testbed.deactivate()

class TestTest(TestBase):
    def testInsertEntity(self):
        """
        Test the test cases.
        """
        TestModel().put()
        self.assertEqual(1, len(TestModel.query().fetch(2)))
