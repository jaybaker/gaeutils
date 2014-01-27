from google.appengine.api import apiproxy_stub_map

import logging

import gaeutils

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
        ns = gaeutils.App.deploy_time
        NS_SEP = '#@'

        # looks for a Set operation
        # checks config and also safety checks not in prod
        if call in ('Set',) and not use_cache and gaeutils.App.dev:
            logging.debug('circumventing cache')
            # get around cache by prepending to namespace on Set
            ns = 'zz' + ns
        # augment namespace
        if callable(getattr(request, 'set_name_space', None)):
            request.set_name_space(ns if not request.name_space() 
                    else (request.name_space() + NS_SEP + ns))

    apiproxy_stub_map.apiproxy.GetPreCallHooks().Append('memcache_ver_namespace', 
            hook, 
            'memcache')
