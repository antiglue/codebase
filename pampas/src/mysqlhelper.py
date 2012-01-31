# vim:syntax=python tabstop=4 shiftwidth=4 expandtab

import MySQLdb
import MySQLdb.cursors
import kronos
import unittest
import threading
import time
import pampas
import traceback
import logging

logger = logging.getLogger('pampas.mysqlhelper')

class MySQLDbInserter(object):
    def __init__(self, max_reconnection_attempt = 3, reconnection_delay_seconds = 10):
        self.connection = None
        self.connectionargs = None
        self.cursor = None
        self.max_reconnection_attempt = max_reconnection_attempt
        self.reconnection_delay_seconds = reconnection_delay_seconds
        self.current_attempt = 0
        self.scheduler = kronos.ThreadedScheduler()
        self.schedulerrunning = False # bug in shceduler.running
        self.commitlock = threading.Lock()

    def connect(self, *args, **kwargs):
        self.connectionargs = args, kwargs
        try:
            self.connection = MySQLdb.connect(*args, **kwargs)
            self.cursor = self.connection.cursor()
            self.current_attempt = 0
        except (AttributeError, MySQLdb.OperationalError):
            if self.current_attempt >= self.max_reconnection_attempt:
                raise
            logger.warning("Connection failed. Current attempt:%d" % self.current_attempt)
            logger.info(traceback.format_exc())
            self.current_attempt += 1
            time.sleep(self.reconnection_delay_seconds)
            self.connect(*args, **kwargs)

    def scheduleCommit(self, delay):
        self.scheduler.add_interval_task(action = self.commit,
            taskname = "MySQLDbInserter.commit",
            initialdelay = delay,
            interval = delay,
            processmethod = kronos.method.threaded,
            args = [],
            kw = {}
        )
        if not self.schedulerrunning:
            self.scheduler.start()
            self.schedulerrunning = True

    def commit(self):
        with self.commitlock:
            if not self.connection:
                return
            try:
                self.connection.commit()
            except (AttributeError, MySQLdb.OperationalError):
                self.connect(*self.connectionargs[0], **self.connectionargs[1])
                self.connection.commit()

    def execute(self, stmt, args = None):
        if not self.cursor:
            raise MySQLdb.NotConnectedException()
        try:
            self.cursor.execute(stmt, args)
        except (AttributeError, MySQLdb.OperationalError):
            self.connect(*self.connectionargs[0], **self.connectionargs[1])
            self.cursor.execute(stmt, args)
        return self.cursor

class TestMySQLDbInserter(unittest.TestCase):
    def setUp(self):
        pass

    def testReconnection(self):
        mdb = MySQLDbInserter()
        mdb.connect('localhost', 'liquida', 'liquida', 'liquida')
        mdb.scheduleCommit(3)
        while True:
            print mdb.execute("select VERSION()").fetchone()
            time.sleep(1)

    def tearDown(self):
        pass

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMySQLDbInserter)
    unittest.TextTestRunner(verbosity=2).run(suite)

