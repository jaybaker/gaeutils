from google.appengine.ext import ndb

import tests

import gaeutils

class EmptyModel(ndb.Model):
    pass

class TestQueryExec(tests.TestBase):
    def setUp(self):
        super(TestQueryExec, self).setUp()

        ndb.put_multi([tests.TestModel(number=i) for i in xrange(0, 30)])

        self.query = tests.TestModel.query()

    def testGetByPage(self):
        self.assertEqual(len(self.query.fetch()), 30)

        # normal operation
        query_exec = gaeutils.QueryExec(self.query, batch_size=10)

        num_pages = 0
        for page in query_exec.get_by_page():
            self.assertEqual(len(page), 10)
            num_pages += 1
        self.assertEqual(3, num_pages)

    def testGetByPageEmpty(self):
        query = EmptyModel.query()
        self.assertEqual(len(query.fetch()), 0)

        # no results
        query_exec = gaeutils.QueryExec(query, batch_size=10)
        num_pages = 0
        for page in query_exec.get_by_page():
            self.assertEqual(len(page), 0)
            num_pages += 1
        self.assertEqual(1, num_pages)

    def testGetAll(self):
        query_exec = gaeutils.QueryExec(self.query, batch_size=10)
        self.assertEqual(len(query_exec.get_all()), len(self.query.fetch()))

    def testIter(self):
        """ As an iterator """
        num_pages = 0
        for page in gaeutils.QueryExec(self.query, batch_size=10):
            num_pages += 1
        self.assertEqual(3, num_pages)
