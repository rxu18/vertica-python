from __future__ import print_function, division, absolute_import

from ..message import BackendMessage
from struct import unpack

class LoadBalanceResponse(BackendMessage):
    message_id = b'Y'

    def __init__(self, data):
        BackendMessage.__init__(self)
        unpacked = unpack('!I{0}sx'.format(len(data) - 5), data)
        self.port = unpacked[0]
        self.host = unpacked[1]

    def get_port(self):
        return self.port

    def get_host(self):
        return self.host

BackendMessage.register(LoadBalanceResponse)
