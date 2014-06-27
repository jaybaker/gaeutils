from google.appengine.api import apiproxy_stub_map, memcache, modules
from google.appengine.ext import ndb

import logging

import gaeutils

class cachestate(ndb.Model):
    """
    Use db to synchronize cache state.
    """
    cleartime = ndb.IntegerProperty(default=0)

    @classmethod
    @ndb.transactional
    def get(cls):
        key = ndb.Key(cls, modules.get_current_module_name())
        inst = key.get()
        if not inst:
            inst = cls(key=key)
            inst.put()
        return inst

def flushcache():
    # conditionally flush memcache if this is loaded
    # due to module deployment
    _cachestate = cachestate.get()
    if _cachestate.cleartime != gaeutils.App.deploy_time:
        logging.debug('flushing cache. last cleared %i and curr deploy time %i' % (
            _cachestate.cleartime, gaeutils.App.deploy_time))
        def txn():
            _cachestate.cleartime = gaeutils.App.deploy_time
            _cachestate.put()
        if memcache.flush_all():
            ndb.transaction(txn)
flushcache()

def memcache_hook(use_cache=True):
    """
    To turn on this memcache hook, place an import early in the 
    import cycle of your app, e.g.
    import gaeutils
    gaeutils.caching.memcache_hook()
    """

    def hook(service, call, request, response):
        """
        Use app version to namespace all calls to memcache.
        This is something like flushing memcache each time 
        you deploy to GAE.
        It also supports globally turning caching off for
        development purposes.
        """
        ns = None
        NS_SEP = '#@'

        # looks for a Set operation
        # checks config and also safety checks not in prod
        if call in ('Set',) and not use_cache and gaeutils.App.dev:
            # get around cache by prepending to namespace on Set
            ns = 'zz' + str(gaeutils.App.deploy_time)
        # augment namespace
        if callable(getattr(request, 'set_name_space', None)):
            namespace = request.name_space()
            if ns:
                namespace = namespace  + NS_SEP + ns if namespace else ns
                request.set_name_space(namespace)

    apiproxy_stub_map.apiproxy.GetPreCallHooks().Append('memcache_ver_namespace', 
            hook, 
            'memcache')
