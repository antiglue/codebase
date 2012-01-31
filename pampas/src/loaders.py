# vim:syntax=python tabstop=4 shiftwidth=4 expandtab

import unittest
import time
import core
import logging
from ConfigParser import ConfigParser

logger = logging.getLogger('pampas.loaders')

def transformdict( fromdict, keymapping, dropunmapped = False):
    for fromkey, value in fromdict.items():
        if fromkey in keymapping:
            value = fromdict.pop(fromkey)
            fromdict[keymapping[fromkey]] = value
        elif dropunmapped:
            del fromdict[fromkey]
    return fromdict

class Loader(object):
    """
    A Loader send datasource query responses to a destination
    """
    def __init__(self, producer, datasource, filters = None, defaultheaders = None):
        self.producer = producer
        self.datasource = datasource
        self.filters = filters
        self.defaultheaders = defaultheaders or {}
        self._interceptcb = None

    def send(self, stmt):
        fetchedcnt = 0
        sentcnt = 0
        if self.filters is None:
            for fetchedcnt, result in enumerate(self.datasource.queryiter(stmt)):
                self._send(result)
            sentcnt = fetchedcnt
        else:
            for fetchedcnt, result in enumerate(self.datasource.queryiter(stmt)):
                for pos, filt in enumerate(self.filters):
                    if filt(result):
                        logger.debug("Result filtered by rule %d" % pos)
                    else:
                        sentcnt += 1
                        self._send(result)
        logger.info("Fetched %d records from datasource. Sent %s to producer" % 
                    (fetchedcnt, sentcnt))

    def intercept(self, callback):
        self._interceptcb = callback

    def _intercept(self, message, headers):
        if self._interceptcb:
            return self._interceptcb(message, headers)
        else:
            return message, headers

    def _send(self, result):
        headers = self.defaultheaders.copy()
        message, headers = self._intercept(result, headers)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Sending: h:%s, m:%s" % (headers, message))
        self.producer.sendMessage(message=message, headers=headers)
        
class PKFieldNotInResultSetError(Exception):
    pass

class IncrementalLoader(Loader):
    """
    An IncrementalLoader implements delta import functionallity. 
    It knows how to store and load pk fields value to file
    """
    DIMP_LAST = 'last'
    DIMP_TSTAMP = 'timestamp'

    def __init__(self, producer, datasource, pkfield, pkvalue = None):
        Loader.__init__(self, producer, datasource)
        self.pkfield = pkfield
        self.pkvalue = pkvalue

    def send(self, stmt):
        logger.debug("%s => %s" % (stmt, self.pkvalue))
        try:
            stmt = self.datasource.bind(stmt, self.pkvalue)
            logger.debug("stmt final: %s" % stmt)
        except TypeError, ex:
            logger.warning("Error composing incremental stmt \"%s\": %s" % (stmt, self.pkvalue) )
            raise ex
        super(IncrementalLoader, self).send(stmt)

    def _send(self, result):
        super(IncrementalLoader, self)._send(result)
        self.pkvalue = max(result.get(self.pkfield, None), self.pkvalue)

    def store(self, filename, section):
        configparser = ConfigParser()
        configparser.read(filename)
        if not configparser.has_section(section):
            configparser.add_section(section)
        logger.debug("Last value: %s" % self.pkvalue)
        configparser.set(
            section, 
            self.DIMP_LAST, 
            self.pkvalue
        )
        configparser.set(
            section, 
            self.DIMP_TSTAMP, 
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        )
        with open(filename, 'w') as configfile:
            configparser.write(configfile)

    def load(self, filename, section, pktype = 'str', pkdefault = None ):
        configparser = ConfigParser()
        if configparser.read(filename) and configparser.has_section(section):
            self.pkvalue = eval(pktype)(configparser.get(section, self.DIMP_LAST))
        else:
            self.pkvalue = pkdefault
        logger.debug("Read maxvalue %s from %s.%s" % (str(self.pkvalue), section, self.DIMP_LAST))


import datasources
class TestLoader(unittest.TestCase):
    destination = 'testqueue'
    testresponse = [{'Id':i} for i in range(10)]

    def setUp(self):
        self.factory = core.MockFactory()
        self.factory.setMessageQueue(self.destination)
        self.datasource = datasources.MockDataSource(self.testresponse)
        self.datasource.connect()

    def testSend(self):
        producer = self.factory.createProducer()
        producer.connect()
        stmt = "SELECT '1' as Id, 'testo' as Text, 1234 as 'IntVal' FROM DUAL"
        mp = Loader(producer, self.datasource)
        mp.send(stmt, {'Text':'Testo'})
        self.assertEqual(len(producer.messages), len(self.testresponse))

    def testSendFilters(self):
        def filter_odds(message):
            if message['Id'] % 2:
                return True
        producer = self.factory.createProducer()
        producer.connect()
        stmt = "SELECT '1' as Id, 'testo' as Text, 1234 as 'IntVal' FROM DUAL"
        mp = Loader(producer, self.datasource, filters=[filter_odds])
        mp.send(stmt, {'Text':'Testo'})
        self.assertEqual(len(producer.messages), len(self.testresponse) / 2)

    def testSendIntercept(self):
        def addid2headers(message, headers):
            headers['myid'] = message['Id']
            return message, headers
        producer = self.factory.createProducer()
        producer.connect()
        stmt = "SELECT '1' as Id, 'testo' as Text, 1234 as 'IntVal' FROM DUAL"
        mp = Loader(producer, self.datasource)
        mp.intercept(addid2headers)
        mp.send(stmt, {'Text':'Testo'})
        self.assertEqual(len([1 for message, headers in producer.messages if 'myid' in headers]), len(self.testresponse))

    def tearDown(self):
        self.datasource.disconnect()
        #self.factory.disconnectAll()

def main():
    TEMPLATE = "http://liqit-1-dbm:8080/solr/imgit2/select/?q=$QUERY&sort=DocDatetime%20desc"
    """
    datasource = datasources.SolrDataSource(TEMPLATE, rows = 5)
    datasource.connect()
    testfactory = core.TestFactory('./filequeues')
    testfactory.setMessageQueue('/queue/testqueue')
    fileproducer = testfactory.createProducer()
    loader = IncrementalLoader(fileproducer, datasource, 'Id' )
    #loader = self.getLoader()
    section = 'test'
    pktype = 'long'
    loader.load('dataimport-test.properties', section, pktype)
    loader.send("Tags:ipad")
    loader.store('dataimport-test.properties', section)
    """

    suite = unittest.TestLoader().loadTestsFromTestCase(TestLoader)
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == '__main__':
    main()

