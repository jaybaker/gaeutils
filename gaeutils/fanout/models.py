import uuid
import base64

from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from google.appengine.ext import deferred

from gaeutils import safe_enqueue

_SHARD_SIZE = 100          # subscribers per shard
_SHARD_CHILDREN_LIMIT = 3  # limit children of shard

class Shard(ndb.Model):
    subscribers       = ndb.KeyProperty(repeated=True)
    subscriber_count  = ndb.ComputedProperty(lambda self: len(self.subscribers))
    # to efficiently find other shards, a given shard 
    # may be linked to others. When work executes, a 
    # shard looks for these other shards
    # the key is not to have too many shards linked to any given shard
    shard_child_count = ndb.IntegerProperty(default=0) # this doesn't need to be very accurate

    # used for queries
    depth             = ndb.IntegerProperty(default=0)

    def _pre_put_hook(self):
        self.depth = len(self.key.pairs()) - 2

    @property
    def children(self):
        query = Shard.query(ancestor = self.key)
        # get first level of children
        query = query.filter(Shard.depth == self.depth + 1)
        return query.fetch(keys_only=True)

    @property
    def subscription(self):
        # subscription is the top parent
        return ndb.Key(pairs=[ self.key.pairs()[0] ])

    def do_work(self, shard_url, work_url, **kwargs):
        """
        Additional params to the task can be passed as a
        dict named 'params' in kwargs.
        """
        params = kwargs.pop('params', {})
        # job_id gets passed along
        params.update(shard_url=shard_url, 
                work_url=work_url, 
                subscription=self.subscription.urlsafe())
        # kick off children
        for child in self.children:
            params.update(dict(shard=child.urlsafe()))
            safe_enqueue(shard_url, 
                    params=params, 
                    name='%s-%s' % (params['job_id'], child.id()), 
                    **kwargs)

        params['subscriber_keys'] = [key.urlsafe() for key in self.subscribers]
        safe_enqueue(work_url, params=params, **kwargs)

    @ndb.transactional
    def add_subscriber(self, ref):
        self.subscribers = list(set(self.subscribers + [ref]))
        self.put()

    @classmethod
    def find_shard(cls, subscription, shard_size=_SHARD_SIZE, shard_child_limit=_SHARD_CHILDREN_LIMIT):
        if not isinstance(subscription, ndb.Key):
            subscription = subscription.key

        def incr_shard_count(_shard):
            # not doing this in a trasaction because the shard count doesn't need 
            # to be accurate; just so it is 0 or some number close to actual child count
            _shard.shard_child_count += 1
            _shard.put()

        base_query = cls.query(ancestor = subscription)
        # get one that has room
        shard = base_query.filter(cls.subscriber_count < shard_size).get()
        if shard is None: # creating new shard
            parent_query = base_query.filter(cls.shard_child_count < shard_child_limit)
            parent_shard = parent_query.order(-cls.shard_child_count).order(cls.depth).get()
            if parent_shard is not None:
                # there is a shard to serve as parent
                shard = Shard(parent=parent_shard.key)
                incr_shard_count(parent_shard)
            else:
                shard = Shard(parent=subscription)
            shard.put()

        return shard


class Subscription(ndb.Model):
    def subscribe(self, ref, shard_size=_SHARD_SIZE, shard_child_limit=_SHARD_CHILDREN_LIMIT):
        if not isinstance(ref, ndb.Key):
            ref = ref.key
        existing = self.shards(ref=ref, limit=1)
        if existing is None:
            shard = Shard.find_shard(self, shard_size=shard_size, shard_child_limit=shard_child_limit)
            shard.add_subscriber(ref)

    def unsubscribe(self, ref):
        existing = self.shards(ref=ref)
        if len(existing) > 0:
            for shard in existing:
                shard.subscribers = [sub for sub in shard.subscribers if sub != ref]
                shard.put()

    def shards(self, ref=None, limit=500):
        """
        Mainly for testing. Get all shards for this subscription.
        """
        query = Shard.query(ancestor = self.key)
        if ref is not None:
            query = query.filter(Shard.subscribers == ref)
        if limit == 1:
            return query.get()
        else:
            return query.fetch(limit)

    def do_work(self, shard_url, **kwargs):
        """
        Additional params to the task can be passed as a
        dict named 'params' in kwargs.
        """
        job_id = base64.b32encode(uuid.uuid4().bytes).strip('=').lower()
        # first tier of shards
        shards = Shard.query(ancestor = self.key).filter(Shard.depth == 0).fetch(keys_only=True)
        params = kwargs.pop('params', {})
        params.update(dict(shard_url=shard_url, 
            subscription=self.key.urlsafe()),
            job_id=job_id)
        for shard in shards:
            params.update(dict(shard=shard.urlsafe()))
            safe_enqueue(shard_url, 
                    params=params, 
                    name='%s-%s' % (job_id, shard.id()), 
                    **kwargs)
