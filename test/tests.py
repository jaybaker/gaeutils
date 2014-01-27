import unittest
from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.ext import testbed

import gaeutils

class TestModel(ndb.Model):
    """A model class used for testing."""
    number = ndb.IntegerProperty(default=42)
    text   = ndb.StringProperty()

class TestBase(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()

        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_app_identity_stub()

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
