from ..message import BulkFrontendMessage
from struct import pack

class LoadBalanceRequest(BulkFrontendMessage):
    def __init__(self):
        BulkFrontendMessage.__init__(self)

    def get_message(self):
        bytes_ = pack('!i2h', 8, 1235, 0)
        return bytes_