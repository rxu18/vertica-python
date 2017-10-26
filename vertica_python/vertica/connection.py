from __future__ import print_function, division, absolute_import

import logging
import socket
import ssl
import os
import errno
from struct import unpack

# noinspection PyCompatibility,PyUnresolvedReferences
from builtins import str
from six import raise_from, string_types, integer_types

from .. import errors
from ..vertica import messages
from ..vertica.cursor import Cursor
from ..vertica.messages.message import BackendMessage, FrontendMessage
from ..vertica.messages.frontend_messages import CancelRequest

from collections import deque

logger = logging.getLogger('vertica')

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_PATH = 'vertica_python.log'
ASCII = 'ascii'
DEFAULT_PORT = 5433


def connect(**kwargs):
    """Opens a new connection to a Vertica database."""
    return Connection(kwargs)


def ensure_dir_exists(filepath):
    """Ensure that a directory exists

    If it doesn't exist, try to create it and protect against a race condition
    if another process is doing the same.
    """
    directory = os.path.dirname(filepath)
    if directory != '' and not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise


class _AddressList(object):
    def __init__(self, host, port, backup_nodes):
        """Creates a new deque with the primary host first, followed by any backup hosts"""
        # Format of items in deque: (host, port, is_dns_resolved)
        self.address_deque = deque()

        # load primary host into address_deque
        self._append(host, port)

        # load backup nodes into address_deque
        if not isinstance(backup_nodes, list):
            err_msg = 'Connection option "backup_server_node" must be a list'
            logger.error(err_msg)
            raise TypeError(err_msg)

        # Each item in backup_nodes should be either
        # a host name or IP address string (using default port) or
        # a (host, port) tuple
        for node in backup_nodes:
            if isinstance(node, string_types):
                self._append(node, DEFAULT_PORT)
            elif isinstance(node, tuple) and len(node) == 2:
                self._append(node[0], node[1])
            else:
                err_msg = ('Each item of connection option "backup_server_node"'
                           ' must be a host string or a (host, port) tuple')
                logger.error(err_msg)
                raise TypeError(err_msg)

    def _append(self, host, port):
        if isinstance(host, string_types) and isinstance(port, integer_types):
            self.address_deque.append((host, port, False))
        else:
            err_msg = 'Host {0} must be a string and port {1} must be an integer'.format(host, port)
            logger.error(err_msg)
            raise TypeError(err_msg)

    def push(self, host, port):
        self.address_deque.appendleft((host, port, False))

    def pop(self):
        self.address_deque.popleft()

    def peek(self):
        # do lazy DNS resolution, return the leftmost DNS-resolved address
        if len(self.address_deque) == 0:
            return None

        while len(self.address_deque) > 0:
            host, port, is_dns_resolved = self.address_deque[0]
            if is_dns_resolved:
                # return a resolved address
                logger.debug('Peek at address list: {0}'.format(list(self.address_deque)))
                return (host, port)
            else:
                # DNS resolve a single host name to multiple IP addresses
                self.address_deque.popleft()
                try:
                    resolved_hosts = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
                except Exception as e:
                    logger.warning('Error resolving host {0} on port {1}: {2}'.format(host, port, e))
                    continue

                # add resolved IP addresses to deque
                for res in reversed(resolved_hosts):
                    family, socktype, proto, canonname, sockaddr = res
                    self.address_deque.appendleft((sockaddr[0], sockaddr[1], True))

        return None


class Connection(object):
    def __init__(self, options=None):
        self.parameters = {}
        self.session_id = None
        self.backend_pid = None
        self.backend_key = None
        self.transaction_status = None
        self.socket = None

        options = options or {}
        self.options = {key: value for key, value in options.items() if value is not None}

        # we only support one cursor per connection
        self.options.setdefault('unicode_error', None)
        self._cursor = Cursor(self, None, unicode_error=self.options['unicode_error'])
        self.options.setdefault('port', DEFAULT_PORT)
        self.options.setdefault('read_timeout', 600)

        # Set up logger
        if 'log_level' not in self.options and 'log_path' not in self.options:
            # logger is disabled by default
            logger.disabled = True
        else:
            self.options.setdefault('log_level', DEFAULT_LOG_LEVEL)
            self.options.setdefault('log_path', DEFAULT_LOG_PATH)
            ensure_dir_exists(self.options['log_path'])
            logging.basicConfig(datefmt='%Y-%m-%d %I:%M:%S',
                    format='%(asctime)s.%(msecs)03d [%(module)s] <%(levelname)s> %(message)s',
                    level=self.options['log_level'],
                    filename=self.options['log_path'])

        for required_option in ('host', 'database', 'user', 'password'):
            if required_option not in self.options:
                err_msg = 'Connection option "{0}" is required'.format(required_option)
                logger.error(err_msg)
                raise errors.ConnectionError(err_msg)
        self.address_list = _AddressList(self.options['host'], self.options['port'],
                                         self.options.get('backup_server_node', []))

        logger.info('Connecting as user {0} to database {1} on host {2} and port {3}'.format(
                     self.options['user'], self.options['database'],
                     self.options['host'], self.options['port']))
        self.startup_connection()
        logger.info('Connection is ready')

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        try:
            # if there's no outstanding transaction, we can simply close the connection
            if self.transaction_status in (None, 'in_transaction'):
                return

            if type_ is not None:
                self.rollback()
            else:
                self.commit()
        finally:
            self.close()

    #############################################
    # dbapi methods
    #############################################
    def close(self):
        try:
            self.write(messages.Terminate())
        finally:
            self.close_socket()

    def cancel(self):
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        self.write(CancelRequest(backend_pid=self.backend_pid, backend_key=self.backend_key))

    def commit(self):
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        cur = self.cursor()
        cur.execute('COMMIT;')

    def rollback(self):
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        cur = self.cursor()
        cur.execute('ROLLBACK;')

    def cursor(self, cursor_type=None):
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        if self._cursor.closed():
            self._cursor._closed = False

        # let user change type if they want?
        self._cursor.cursor_type = cursor_type
        return self._cursor

    #############################################
    # internal
    #############################################
    def reset_values(self):
        self.parameters = {}
        self.session_id = None
        self.backend_pid = None
        self.backend_key = None
        self.transaction_status = None
        self.socket = None
        self.address_list = _AddressList(self.options['host'], self.options['port'],
                                         self.options.get('backup_server_node', []))

    def _socket(self):
        if self.socket:
            return self.socket

        # the initial establishment of the client connection
        raw_socket = self.establish_connection()

        # enable load balancing
        load_balance_options = self.options.get('connection_load_balance')
        logger.debug('Connection load balance option is {0}'.format(
                     'enabled' if load_balance_options else 'disabled'))
        if load_balance_options:
            raw_socket = self.balance_load(raw_socket)

        # enable SSL
        ssl_options = self.options.get('ssl')
        logger.debug('SSL option is {0}'.format('enabled' if ssl_options else 'disabled'))
        if ssl_options:
            raw_socket = self.enable_ssl(raw_socket, ssl_options)

        self.socket = raw_socket
        return self.socket

    def create_socket(self):
        # Address family IPv6 (socket.AF_INET6) is not supported
        raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        connection_timeout = self.options.get('connection_timeout')
        if connection_timeout is not None:
            logger.debug('Set socket connection timeout: {0}'.format(connection_timeout))
            raw_socket.settimeout(connection_timeout)
        return raw_socket

    def balance_load(self, raw_socket):
        # Send load balance request and read server response
        raw_socket.sendall(messages.LoadBalanceRequest().get_message())
        response = raw_socket.recv(1)

        if response in (b'Y', 'Y'):
            size = unpack('!I', raw_socket.recv(4))[0]
            if size < 4:
                err_msg = "Bad message size: {0}".format(size)
                logger.error(err_msg)
                raise errors.MessageError(err_msg)
            res = BackendMessage.from_type(type_=response, data=raw_socket.recv(size-4))
            host = res.get_host()
            port = res.get_port()
            logger.info('Load balancing to host {0} on port {1}'.format(host, port))

            socket_host, socket_port = raw_socket.getpeername()
            if host == socket_host and port == socket_port:
                logger.info('Already connecting to host {0} on port {1}. Ignore load balancing.'.format(host, port))
                return raw_socket

            # Push the new host onto the address list before connecting again. Note that this
            # will leave the originally-specified host as the first failover possibility.
            self.address_list.push(host, port)
            raw_socket.close()
            raw_socket = self.establish_connection()
        else:
            logger.warning("Load balancing requested but not supported by server")

        return raw_socket

    def enable_ssl(self, raw_socket, ssl_options):
        from ssl import CertificateError, SSLError
        # Send SSL request and read server response
        raw_socket.sendall(messages.SslRequest().get_message())
        response = raw_socket.recv(1)
        if response in ('S', b'S'):
            logger.info('Enabling SSL')
            try:
                if isinstance(ssl_options, ssl.SSLContext):
                    host, port = raw_socket.getpeername()
                    raw_socket = ssl_options.wrap_socket(raw_socket, server_hostname=host)
                else:
                    raw_socket = ssl.wrap_socket(raw_socket)
            except CertificateError as e:
                raise_from(errors.ConnectionError, e)
            except SSLError as e:
                raise_from(errors.ConnectionError, e)
        else:
            err_msg = "SSL requested but not supported by server"
            logger.error(err_msg)
            raise errors.SSLNotSupported(err_msg)
        return raw_socket

    def establish_connection(self):
        addr = self.address_list.peek()
        raw_socket = None
        last_exception = None

        # Failover: loop to try all addresses
        while addr:
            last_exception = None
            host, port = addr

            logger.info("Establishing connection to host {0} on port {1}".format(host, port))
            try:
                raw_socket = self.create_socket()
                raw_socket.connect((host, port))
                break
            except Exception as e:
                logger.info('Failed to connect to host {0} on port {1}: {2}'.format(host, port, e))
                last_exception = e
                self.address_list.pop()
                addr = self.address_list.peek()
                raw_socket.close()

        # all of the addresses failed
        if raw_socket is None or last_exception:
            err_msg = 'Failed to establish a connection to the primary server or any backup address.'
            logger.error(err_msg)
            raise errors.ConnectionError(err_msg)

        return raw_socket

    def ssl(self):
        return self.socket is not None and isinstance(self.socket, ssl.SSLSocket)

    def opened(self):
        return (self.socket is not None
                and self.backend_pid is not None
                and self.transaction_status is not None)

    def closed(self):
        return not self.opened()

    def write(self, message):
        if not isinstance(message, FrontendMessage):
            raise TypeError("invalid message: ({0})".format(message))

        logger.debug('=> %s', message)
        sock = self._socket()
        try:
            for data in message.fetch_message():
                try:
                    sock.sendall(data)
                except Exception:
                    logger.error("couldn't send message")
                    raise

        except Exception as e:
            self.close_socket()
            if str(e) == 'unsupported authentication method: 9':
                raise errors.ConnectionError(
                    'Error during authentication. Your password might be expired.')
            else:
                # noinspection PyTypeChecker
                raise_from(errors.ConnectionError, e)

    def close_socket(self):
        try:
            if self.socket is not None:
                self._socket().close()
        finally:
            self.reset_values()

    def reset_connection(self):
        self.close()
        self.startup_connection()

    def read_message(self):
        try:
            type_ = self.read_bytes(1)
            size = unpack('!I', self.read_bytes(4))[0]

            if size < 4:
                raise errors.MessageError("Bad message size: {0}".format(size))
            message = BackendMessage.from_type(type_, self.read_bytes(size - 4))
            logger.debug('<= %s', message)
            return message
        except (SystemError, IOError) as e:
            self.close_socket()
            # noinspection PyTypeChecker
            raise_from(errors.ConnectionError, e)

    def process_message(self, message):
        if isinstance(message, messages.ErrorResponse):
            raise errors.ConnectionError(message.error_message())
        elif isinstance(message, messages.NoticeResponse):
            if getattr(self, 'notice_handler', None) is not None:
                self.notice_handler(message)
        elif isinstance(message, messages.BackendKeyData):
            self.backend_pid = message.pid
            self.backend_key = message.key
        elif isinstance(message, messages.ParameterStatus):
            self.parameters[message.name] = message.value
        elif isinstance(message, messages.ReadyForQuery):
            self.transaction_status = message.transaction_status
        elif isinstance(message, messages.CommandComplete):
            # TODO: I'm not ever seeing this actually returned by vertica...
            # if vertica returns a row count, set the rowcount attribute in cursor
            # if hasattr(message, 'rows'):
            #     self.cursor.rowcount = message.rows
            pass
        elif isinstance(message, messages.EmptyQueryResponse):
            pass
        elif isinstance(message, messages.CopyInResponse):
            pass
        else:
            raise errors.MessageError("Unhandled message: {0}".format(message))

        # set last message
        self._cursor._message = message

    def __str__(self):
        safe_options = {key: value for key, value in self.options.items() if key != 'password'}

        s1 = "<Vertica.Connection:{0} parameters={1} backend_pid={2}, ".format(
            id(self), self.parameters, self.backend_pid)
        s2 = "backend_key={0}, transaction_status={1}, socket={2}, options={3}>".format(
            self.backend_key, self.transaction_status, self.socket, safe_options)
        return ''.join([s1, s2])

    def read_bytes(self, n):
        results = bytes()
        while len(results) < n:
            bytes_ = self._socket().recv(n - len(results))
            if not bytes_:
                raise errors.ConnectionError("Connection closed by Vertica")
            results += bytes_
        return results

    def startup_connection(self):
        # This doesn't handle Unicode usernames or passwords
        user = self.options['user'].encode(ASCII)
        database = self.options['database'].encode(ASCII)
        password = self.options['password'].encode(ASCII)

        self.write(messages.Startup(user, database))

        while True:
            message = self.read_message()

            if isinstance(message, messages.Authentication):
                # Password message isn't right format ("incomplete message from client")
                if message.code != messages.Authentication.OK:
                    self.write(messages.Password(password, message.code,
                                                 {'user': user,
                                                  'salt': getattr(message, 'salt', None)}))
            else:
                self.process_message(message)

            if isinstance(message, messages.ReadyForQuery):
                break
