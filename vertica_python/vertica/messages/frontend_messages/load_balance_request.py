from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class LoadBalanceRequest(BulkFrontendMessage):
    message_id = None
    LOADBALANCE_REQUEST = 80936960

    def read_bytes(self):
        bytes_ = pack('!I', self.LOADBALANCE_REQUEST)
        return bytes_
