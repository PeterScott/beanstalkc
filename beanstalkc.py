#!/usr/bin/env python
"""beanstalkc - A beanstalkd Client Library for Python"""

__license__ = '''
Copyright (C) 2008-2010 Andreas Bolka

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

__version__ = '4.0.1'

import logging
import socket
import re
import math, random, time
from threading import Lock


DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 11300
DEFAULT_PRIORITY = 2**31
DEFAULT_TTR = 120
DEFAULT_TIMEOUT = 0.05          # 50 ms
DEFAULT_UPPER_BACKOFF_BOUND = 30


class BeanstalkcException(Exception): pass
class UnexpectedResponse(BeanstalkcException): pass
class CommandFailed(BeanstalkcException): pass
class DeadlineSoon(BeanstalkcException): pass
class SocketError(BeanstalkcException): pass


class Connection(object):
    """A connection to a beanstalk daemon."""
    
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT,
                 connection_timeout=DEFAULT_TIMEOUT,
                 reconnect_strategy=None,
                 upper_backoff_bound=DEFAULT_UPPER_BACKOFF_BOUND):
        """Create connection. Note that if reconnect strategy is
        specified, you must call connect() manually."""
        self._socket = None
        self.tube = 'default'
        self.host = host
        self.port = port
        self.lock = Lock()
        self.connection_timeout = connection_timeout
        self.unsuccessful_connects = 0
        self.upper_backoff_bound = upper_backoff_bound
        if reconnect_strategy in [None, 'exp_backoff', 'constant']:
            self.reconnect_strategy = reconnect_strategy
        else:
            raise ValueError('Reconnect strategy must be None, "exp_backoff", or "constant"')
        if reconnect_strategy is None:
            self.connect()

    def _current_wait_time(self):
        """Return amount of time (in seconds) to wait for a connection, and
        between subsequent reconnection attempts."""
        connection_timeout = self.connection_timeout or DEFAULT_TIMEOUT
        if self.reconnect_strategy is None:
            return self.connection_timeout
        elif self.reconnect_strategy == 'exp_backoff':
            r = random.uniform(1.0, 2.0)
            return min(r * connection_timeout * math.pow(2.0, self.unsuccessful_connects),
                       self.upper_backoff_bound)
        elif self.reconnect_strategy == 'constant':
            return connection_timeout
        else:
            raise ValueError('Reconnect strategy must be None, "exp_backoff", or "constant"')

    def connect(self):
        """Connect to beanstalkd server, unless already connected."""
        if not self.closed:
            return
        while True:
            try:
                if self.unsuccessful_connects > 0 and self.reconnect_strategy is not None:
                    time.sleep(self._current_wait_time())
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.settimeout(self.connection_timeout and self._current_wait_time())
                self._socket.connect((self.host, self.port))
                self._socket.settimeout(None)
                self._socket_file = self._socket.makefile('rb')
                self.unsuccessful_connects = 0
                return
            except socket.error, e:
                self.unsuccessful_connects += 1
                self._socket = None

    def close(self):
        """Close connection to server, if it is open."""
        if self.closed:
            return
        try:
            self._socket.sendall('quit\r\n')
            self._socket.close()
        except socket.error:
            pass
        finally:
            self._socket = None

    @property
    def closed(self):
        return self._socket is None

    def _interact(self, command, expected_ok, expected_err=[], size_field=None):
        with self.lock:
            while True:
                self.connect()
                try:
                    self._socket.sendall(command)
                    status, results = self._read_response()
                    if status in expected_ok:
                        if size_field is not None:
                            results.append(self._read_body(int(results[size_field])))
                        return results
                    elif status in expected_err:
                        raise CommandFailed(command.split()[0], status, results)
                    else:
                        raise UnexpectedResponse(command.split()[0], status, results)
                except socket.error, e:
                    self.close()

    def _read_response(self):
        line = self._socket_file.readline()
        if not line:
            raise socket.error('no data read')
        response = line.split()
        return response[0], response[1:]

    def _read_body(self, size):
        body = self._socket_file.read(size)
        self._socket_file.read(2) # trailing crlf
        if size > 0 and not body:
            raise socket.error('no data read')
        return body

    def _interact_value(self, command, expected_ok, expected_err=[]):
        return self._interact(command, expected_ok, expected_err)[0]

    def _interact_job(self, command, expected_ok, expected_err, reserved=True):
        jid, _, body = self._interact(command, expected_ok, expected_err,
                                      size_field=1)
        return Job(self, int(jid), body, reserved)

    def _interact_yaml_dict(self, command, expected_ok, expected_err=[]):
        _, body, = self._interact(command, expected_ok, expected_err,
                                  size_field=0)
        return parse_yaml_dict(body)

    def _interact_yaml_list(self, command, expected_ok, expected_err=[]):
        _, body, = self._interact(command, expected_ok, expected_err,
                                  size_field=0)
        return parse_yaml_list(body)

    def _interact_peek(self, command):
        try:
            return self._interact_job(command, ['FOUND'], ['NOT_FOUND'], False)
        except CommandFailed, (_, status, results):
            return None

    # -- public interface --

    def put(self, body, priority=DEFAULT_PRIORITY, delay=0, ttr=DEFAULT_TTR, tube=None):
        """Put a job into the current tube. Returns job id. If you
        specify a different tube, it will change the current tube."""
        assert isinstance(body, str), 'Job body must be a str instance'
        if tube:
            self.use(tube)
        jid = self._interact_value(
                'put %d %d %d %d\r\n%s\r\n' %
                    (priority, delay, ttr, len(body), body),
                ['INSERTED', 'BURIED'], ['JOB_TOO_BIG'])
        return int(jid)

    def reserve(self, timeout=None):
        """Reserve a job from one of the watched tubes, with optional timeout in
        seconds. Returns a Job object, or None if the request times out."""
        if timeout is not None:
            command = 'reserve-with-timeout %d\r\n' % timeout
        else:
            command = 'reserve\r\n'
        try:
            return self._interact_job(command,
                                      ['RESERVED'],
                                      ['DEADLINE_SOON', 'TIMED_OUT'])
        except CommandFailed, (_, status, results):
            if status == 'TIMED_OUT':
                return None
            elif status == 'DEADLINE_SOON':
                raise DeadlineSoon(results)

    def kick(self, bound=1):
        """Kick at most bound jobs into the ready queue."""
        return int(self._interact_value('kick %d\r\n' % bound, ['KICKED']))

    def peek(self, jid):
        """Peek at a job. Returns a Job, or None."""
        return self._interact_peek('peek %d\r\n' % jid)

    def peek_ready(self):
        """Peek at next ready job. Returns a Job, or None."""
        return self._interact_peek('peek-ready\r\n')

    def peek_delayed(self):
        """Peek at next delayed job. Returns a Job, or None."""
        return self._interact_peek('peek-delayed\r\n')

    def peek_buried(self):
        """Peek at next buried job. Returns a Job, or None."""
        return self._interact_peek('peek-buried\r\n')

    def tubes(self):
        """Return a list of all existing tubes."""
        return self._interact_yaml_list('list-tubes\r\n', ['OK'])

    def using(self):
        """Return the tube currently being used."""
        return self.tube

    def use(self, name):
        """Use a given tube. If you are already using that tube, this
        is a no-op and does not contact the server."""
        if self.tube != name:
            self.tube = name
            return self._interact_value('use %s\r\n' % name, ['USING'])

    def watching(self):
        """Return a list of all tubes being watched."""
        return self._interact_yaml_list('list-tubes-watched\r\n', ['OK'])

    def watch(self, name):
        """Watch a given tube."""
        return int(self._interact_value('watch %s\r\n' % name, ['WATCHING']))

    def ignore(self, name):
        """Stop watching a given tube."""
        try:
            return int(self._interact_value('ignore %s\r\n' % name,
                                            ['WATCHING'],
                                            ['NOT_IGNORED']))
        except CommandFailed:
            return 1

    def stats(self):
        """Return a dict of beanstalkd statistics."""
        return self._interact_yaml_dict('stats\r\n', ['OK'])

    def stats_tube(self, name):
        """Return a dict of stats about a given tube."""
        return self._interact_yaml_dict('stats-tube %s\r\n' % name,
                                        ['OK'],
                                        ['NOT_FOUND'])

    def pause_tube(self, name, delay):
        """Pause a tube for a given delay time, in seconds."""
        self._interact('pause-tube %s %d\r\n' %(name, delay),
                       ['PAUSED'],
                       ['NOT_FOUND'])

    # -- job interactors --

    def delete(self, jid):
        """Delete a job, by job id."""
        self._interact('delete %d\r\n' % jid, ['DELETED'], ['NOT_FOUND'])

    def release(self, jid, priority=DEFAULT_PRIORITY, delay=0):
        """Release a reserved job back into the ready queue."""
        self._interact('release %d %d %d\r\n' % (jid, priority, delay),
                       ['RELEASED', 'BURIED'],
                       ['NOT_FOUND'])

    def bury(self, jid, priority=DEFAULT_PRIORITY):
        """Bury a job, by job id."""
        self._interact('bury %d %d\r\n' % (jid, priority),
                       ['BURIED'],
                       ['NOT_FOUND'])

    def touch(self, jid):
        """Touch a job, by job id, requesting more time to work on a reserved
        job before it expires."""
        self._interact('touch %d\r\n' % jid, ['TOUCHED'], ['NOT_FOUND'])

    def stats_job(self, jid):
        """Return a dict of stats about a job, by job id."""
        return self._interact_yaml_dict('stats-job %d\r\n' % jid,
                                        ['OK'],
                                        ['NOT_FOUND'])


class Job(object):
    def __init__(self, conn, jid, body, reserved=True):
        self.conn = conn
        self.jid = jid
        self.body = body
        self.reserved = reserved

    def _priority(self):
        stats = self.stats()
        if isinstance(stats, dict):
            return stats['pri']
        return DEFAULT_PRIORITY

    # -- public interface --

    def delete(self):
        """Delete this job."""
        self.conn.delete(self.jid)
        self.reserved = False

    def release(self, priority=None, delay=0):
        """Release this job back into the ready queue."""
        if self.reserved:
            self.conn.release(self.jid, priority or self._priority(), delay)
            self.reserved = False

    def bury(self, priority=None):
        """Bury this job."""
        if self.reserved:
            self.conn.bury(self.jid, priority or self._priority())
            self.reserved = False

    def touch(self):
        """Touch this reserved job, requesting more time to work on it before it
        expires."""
        if self.reserved:
            self.conn.touch(self.jid)

    def stats(self):
        """Return a dict of stats about this job."""
        return self.conn.stats_job(self.jid)

def parse_yaml_dict(yaml):
    """Parse a YAML dict, in the form returned by beanstalkd."""
    dict = {}
    for m in re.finditer(r'^\s*([^:\s]+)\s*:\s*([^\s]*)$', yaml, re.M):
        key, val = m.group(1), m.group(2)
        # Check the type of the value, and parse it.
        if key == 'name' or key == 'tube' or key == 'version':
            dict[key] = val   # String, even if it looks like a number
        elif re.match(r'^(0|-?[1-9][0-9]*)$', val) is not None:
            dict[key] = int(val) # Integer value
        elif re.match(r'^(-?\d+(\.\d+)?(e[-+]?[1-9][0-9]*)?)$', val) is not None:
            dict[key] = float(val) # Float value
        else:
            dict[key] = val     # String value
    return dict

def parse_yaml_list(yaml):
    """Parse a YAML list, in the form returned by beanstalkd."""
    return re.findall(r'^- (.*)$', yaml, re.M)

if __name__ == '__main__':
    import doctest, os, signal
    try:
        pid = os.spawnlp(os.P_NOWAIT,
                         'beanstalkd',
                         'beanstalkd', '-l', '127.0.0.1', '-p', '14711')
        time.sleep(0.2)         # Wait for beanstalkd to start
        doctest.testfile('TUTORIAL.md', optionflags=doctest.ELLIPSIS)
        doctest.testfile('test/network.doctest', optionflags=doctest.ELLIPSIS)
    finally:
        os.kill(pid, signal.SIGTERM)
