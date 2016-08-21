import os
import time
from google.appengine.api import taskqueue, app_identity
from google.appengine.ext import ndb

__all__ = ['App', 
    'safe_enqueue', 
    'QueryExec', 
    'urlsafe',
    'fanout',
    'Stack',
    'FutStack']

class _App(object):
    """
    Encapsulates some basic identifying features
    often used.
    """
    def __init__(self):
        self.setup()

    def setup(self):
        """
        Needed for environments like test if the env is not set yet.
        """
        try:
            self.id = app_identity.get_application_id()
        except:
            # call to get_application_id fails in unitest env for some reason
            self.id = os.getenv('APPLICATION_ID')
        version_info     = os.getenv('CURRENT_VERSION_ID', '').split('.')
        self.version     = version_info[0] if len(version_info) > 0 else ''
        self.deploy_time = int(version_info[1]) if len(version_info) > 1 else 0
        self.dev         = os.getenv('SERVER_SOFTWARE', '').startswith('Dev')

    def __str__(self):
        return 'App: id=%s ver=%s deployed=%s dev=%s' % (self.id, 
                self.version, self.deploy_time, self.dev)

App = _App()

def safe_enqueue(url, max_retry_timeout=2000, **kwargs):
    """
    Utility to enqueue a task.
    """
    timeout_ms = 100
    while timeout_ms < max_retry_timeout or max_retry_timeout == 0:
        try:
            taskqueue.add(url=url, **kwargs)
            break
        except taskqueue.TransientError: # try again
            # max timeout of 0 is signal to not retry
            if max_retry_timeout == 0:
                break
            time.sleep(timeout_ms/1000)
            timeout_ms *= 2
        except (taskqueue.TombstonedTaskError, taskqueue.TaskAlreadyExistsError): 
            break # already queued

def urlsafe(key_repr):
    """
    Converts an ndb.Key to urlsafe representation while allowing 
    a string to just pass through.
    This is useful as one often wants to support a function that 
    obligingly accepts either a Key or what is already the 
    urlsafe encoded version.
    Usage: key = ndb.Key(urlsafe=urlsafe(key_info))
    """
    if isinstance(key_repr, basestring):
        return key_repr
    else:
        return key_repr.urlsafe()

class Stack(object):
    """ A base stack implementation. """
    def __init__(self):
        self.stack = []

    def push(self, item):
        """ Supports chaining push calls. """
        self.stack.append(item)
        return self

    def pop(self):
        return self.stack.pop()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        assert len(self) == 0

    def __len__(self):
        return len(self.stack)

class FutStack(Stack):
    """
    For pushing futures.
    get_result() is called on pop.
    """
    def pop(self):
        future = super(FutStack, self).pop()
        return future.get_result()

class PageFuture(object):
    """
    A specific proxy for the future returned from fetch_page_async.
    Frequently, just the page results are needed.
    """
    def __init__(self, future):
        self.future = future

    def get_result(self):
        items, next_curs, more = self.future.get_result()
        return items

class QueryExec(object):
    def __init__(self, query, batch_size=300):
        self.query = query
        self.batch_size = batch_size

    def get_by_page_async(self, **kwargs):
        more, next_curs = True, None
        while more:
            future = self.query.fetch_page_async(self.batch_size, start_cursor=next_curs, **kwargs)
            yield PageFuture(future)
            page, next_curs, more = future.get_result()

    def get_by_page(self, **kwargs):
        """
        Generator yielding one page at a time
        """
        for page_fut in self.get_by_page_async(**kwargs):
            items = page_fut.get_result()
            yield items

    @ndb.tasklet
    def get_all_async(self, **kwargs):
        items = []
        more, next_curs = True, None
        while more:
            page, next_curs, more = yield self.query.fetch_page_async(self.batch_size, start_cursor=next_curs, **kwargs)
            items.extend(page)
        raise ndb.Return(items)

    def get_all(self, **kwargs):
        """
        Common operation to get all of something.
        kwargs passed to fetch.
        """
        items = []
        for page in self.get_by_page(**kwargs):
            items.extend(page)
        return items

    def __iter__(self):
        """
        Return the generator as an iterator.
        """
        return self.get_by_page()
