from __future__ import print_function, division, absolute_import

import os
import logging

class VerticaLogging(object):

    @classmethod
    def setup_file_logging(cls, logger_name, logfile, log_level=logging.INFO, context=''):
        logger = logging.getLogger(logger_name)
        formatter = logging.Formatter(
            fmt='%(asctime)s.%(msecs)03d [%(module)s] {}/%(process)d:0x%(thread)x <%(levelname)s> %(message)s'.format(context), 
            datefmt='%Y-%m-%d %H:%M:%S')
        cls.ensure_dir_exists(logfile)
        file_handler = logging.FileHandler(logfile)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.setLevel(log_level)

    @classmethod
    def ensure_dir_exists(cls, filepath):
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
