from google.appengine.ext import ndb

import tests

import gaeutils
from gaeutils import fanout

class SubscriptionTest(fanout.Subscription):
    match = ndb.StringProperty()

class Subscriber(ndb.Model):
    name = ndb.StringProperty()

class TestFanout(tests.TestBase):
    def setUp(self):
        super(TestFanout, self).setUp()
        self.sub = SubscriptionTest(match='abc')
        self.sub.put()
        self.subscriber1 = Subscriber(name='fred')
        self.subscriber2 = Subscriber(name='alice')
        self.subscriber1.put()
        self.subscriber2.put()

    def more_subscribers(self, n=10):
        for i in xrange(0, n):
            subscriber = Subscriber(name=str(i))
            subscriber.put()

        self.subscribers = Subscriber.query().fetch()

    def testSubcribe(self):
        self.sub.subscribe(self.subscriber1)
        shards = self.sub.shards()
        self.assertEqual(1, len(shards))
        self.assertTrue(self.subscriber1.key in shards[0].subscribers)

    def testUnsubscribe(self):
        # unsubscribe w/ no subscription
        self.subscriber3 = Subscriber(name='alice')
        self.subscriber3.put()
        self.sub.unsubscribe(self.subscriber3) # no error

        # regular unsubscribe
        self.sub.subscribe(self.subscriber1)
        self.assertEqual(1, len(self.sub.shards(ref=self.subscriber1.key)))
        self.sub.unsubscribe(self.subscriber1)
        self.assertEqual(0, len(self.sub.shards(ref=self.subscriber1.key)))

    def testSubscribeIdempotent(self):
        self.sub.subscribe(self.subscriber1)
        self.sub.subscribe(self.subscriber1)
        shards = self.sub.shards()
        self.assertEqual(1, len(shards))
        self.assertEqual(1, shards[0].subscriber_count)

    def testSubscriberShardLimit(self):
        _subscriber_size, _shard_num = 4, 3
        self.more_subscribers(n=40)
        for subscriber in self.subscribers:
            self.sub.subscribe(subscriber, shard_size=_subscriber_size, shard_child_limit=_shard_num)
        shards = self.sub.shards()
        self.assertTrue(len(shards) > 1)
        for shard in shards:
            self.assertTrue(shard.subscriber_count <= _subscriber_size)
            self.assertTrue(shard.shard_child_count <= _shard_num)

    def testDoWork(self):
        self.more_subscribers(n=20)
        for subscriber in self.subscribers:
            self.sub.subscribe(subscriber)
        self.sub.do_work('/worker/test', queue_name='default')

    def testDoWorkShard(self):
        # this emulates the webhook at shard_url
        self.more_subscribers(n=20)
        for subscriber in self.subscribers:
            self.sub.subscribe(subscriber)
        shards = self.sub.shards()
        shards[0].do_work('/worker/test', '/worker/accept_subscribers', queue_name='default')

