# gaeutils

A collection of frequently used utilities for the 
Google App Engine Python runtime environment.

Fanout
Requirements:
1. Relatively low cost subscribe mechanism
2. A way to plugin a "match" mechanism
3. On match, an efficient and reasonably parallel method to execute a job per subscriber

Fanout is an implementation which facilitates some form of the twitter model. 
Something happens, and I need to tell 3 million 'people' about it.
The way this works is by breaking up the subscriber space across multiple "shards".
The fanout mechanism kicks in and uses the App Engine taskqueue infrastructure 
to work on each shard in a, more and more, parallel fashion. Each unit of work 
sends a list of references, keys, to a task handler that you specify.

The canonical use case goes like this.
1. subscribe ... subscribe ... subscribe ...
2. Something happens, an event, for example - someone publishes something. 
   You might have a model and subscription(s) that look like:
   class PublishSub(fanout.Subscription):
     author   = ndb.KeyProperty(required=True)
     pub_type = ndb.StringProperty()
3. You query for that and then do subscription.do_work(shard_handler_url)
4. The shard handler basically gets a shard and does shard.do_work(shard_handler_url, work_url)

## TODO

* Add counters
* Add email logger
* Add static content server (css, js)


