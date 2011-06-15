import MySQLdb
import kronos
import unittest
import threading
import time

class MySQLDbInserter(object):
    def __init__(self, max_reconnection_attempt = 3, reconnection_delay_seconds = 10):
        self.connection = None
        self.connectionargs = None
        self.max_reconnection_attempt = max_reconnection_attempt
        self.reconnection_delay_seconds = reconnection_delay_seconds
        self.current_attempt = 0
        self.scheduler = kronos.ThreadedScheduler()
        self.schedulerrunning = False # bug in shceduler.running
        self.commitlock = threading.Lock()

    def connect(self, *args):
        self.connectionargs = args
        try:
            self.connection = MySQLdb.connect(*args)
            self.cursor = self.connection.cursor()
            self.current_attempt = 0
        except (AttributeError, MySQLdb.OperationalError):
            if self.current_attempt >= self.max_reconnection_attempt:
                raise
            logger.warning("Connection failed. Current attempt:%d" % self.current_attempt)
            logger.info(traceback.format_exc())
            self.current_attempt += 1
            time.sleep(self.reconnection_delay_seconds)
            self.connect(*args)

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
        self.commitlock.acquire()
        if not self.connection:
            return
        self.connection.commit()
        self.commitlock.release()

    def execute(self, stmt):
        try:
            self.cursor.execute(stmt)
        except (AttributeError, MySQLdb.OperationalError):
            self.connect(*self.connectionargs)
            self.cursor.execute(stmt)
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

