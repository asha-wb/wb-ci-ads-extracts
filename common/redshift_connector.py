#root/lib/redshift_connector.py
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extensions
import logging


logger = logging.getLogger(__name__)

class LoggingCursor(psycopg2.extensions.cursor):
    def execute(self, sql, args=None):
        logger.debug(self.mogrify(sql, args))  # TODO: remove aws access key and secret key before logging

        try:
            psycopg2.extensions.cursor.execute(self, sql, args)
        except Exception, exc:
            logger.exception("%s: %s" % (exc.__class__.__name__, exc))
            raise


class DBConnection(object):
    def __init__(self, dbname=None, user=None, password=None,
                 host='localhost', port=5439):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

    def __enter__(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                sslmode='require'
            )
        except Exception, exc:
            logger.exception("%s: %s" % (exc.__class__.__name__, exc))
            raise

        return self.connection.cursor(cursor_factory=LoggingCursor)

    def __exit__(self, exec_type, exec_instance, traceback):
        self.connection.close()

