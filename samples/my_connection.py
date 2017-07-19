import logging
from vertica_python import Connection

ASCII = 'ascii'

logger = logging.getLogger('vertica')

class MyConnection(Connection):
    def __init__(self, conn_info):
        Connection.__init__(self, conn_info)