Python beanstalkd client library
====================

beanstalkc is a simple beanstalkd client library for
Python. [beanstalkd](http://kr.github.com/beanstalkd/) is a fast, distributed,
in-memory workqueue service.

beanstalkc is pure Python, and is compatible with
[eventlet](http://eventlet.net/) and [gevent](http://www.gevent.org/).

Installing
-------

To use beanstalkc, you only need the `beanstalkc.py` file. There are no external
dependencies.

Usage
-----

Here is a short example, to illustrate the flavor of beanstalkc:

    >>> import beanstalkc
    >>> beanstalk = beanstalkc.Connection(host='localhost', port=11300)
    >>> beanstalk.put('hey!')
    1
    >>> job = beanstalk.reserve()
    >>> job.body
    'hey!'
    >>> job.delete()

For more information, see `TUTORIAL.md`, which will explain most everything.

License
------

Copyright (C) 2008-2010 Andreas Bolka, Licensed under the [Apache License,
Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
