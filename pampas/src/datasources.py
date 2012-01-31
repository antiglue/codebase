# -*- encoding: utf-8 -*-
# vim:syntax=python tabstop=4 shiftwidth=4 expandtab
#$Id$
#$Date$

"""
Datasource module.
Includes MySQLDataSource, SolrDataSource, SolrFacetDataSource
"""

import MySQLdb
import MySQLdb.cursors
import unittest
import sys
import logging
import urllib
import string
import json
import core
import fetchers

logger = logging.getLogger('pampas.datasources')

class DataSourceError(core.Error):
    pass

class DataSource(object):
    """
    Generic datatasource abstract
    """
    def __init__(self):
        pass

    def connect(self):
        """Initialize datasource connection"""
        pass

    def disconnect(self):
        """connection close"""
        pass

    def query(self, stmt, *parameters):
        """executes the statement"""
        pass

    def literal(self, value):
        """query parameter escape"""
        return value

class MockDataSource(DataSource):
    """
    DataSource mock
    """
    def __init__(self, mockresponse):
        super(MockDataSource, self).__init__()
        self.response = mockresponse

    def query(self, stmt, *parameters):
        logger.info("Executing stmt: %s" % stmt)
        return self.response

class MySQLDataSource(DataSource):
    """
    MySQL interface layer
    """
    def __init__(self, user, passwd, host, db, port = 3306, limit = None, \
                 fetchsize = 50, forceencoding = None):
        super(MySQLDataSource, self).__init__()
        self.connection = None
        self.connectionargs = {'user': user, 'passwd': passwd,
                               'host': host, 'db':db, 'port':int(port)}
        self.forceencoding = forceencoding
        self.limit = limit
        self.fetchsize = fetchsize

    def connect(self):
        logger.debug("connecting to: %s" % (str(self.connectionargs)))
        try:
            self.connection = MySQLdb.connect(**self.connectionargs)
            if self.forceencoding:
                self.connection.set_character_set(self.forceencoding)
        except MySQLdb.Error, exc:
            trace = sys.exc_info()[2]
            raise DataSourceError(
                'Connection failed. Args %s: %s' % (str(self.connectionargs), exc)
            ), None, trace

    def disconnect(self):
        self.connection.close()

    def literal(self, value):
        return self.connection.literal(value)

    #def bind(self, stmt, param):
    #    return stmt % self.literal(param)
    def bind(self, stmt, param, escape=True):
        return string.Template(stmt).safe_substitute({'QUERY':self.literal(param) if escape else param})

    def query(self, stmt, *params):
        return [r for r in self.queryiter(stmt, *params)]

    def queryiter(self, stmt, *params):
        if not self.limit:
            for result in self._queryiter(stmt, *params):
                yield result
        else:
            start = self.limit
            sent = 0
            for result in self._queryiter('%s LIMIT %d %d' % (stmt, start, self.limit), *params):
                sent += 1
                yield result

            while sent < self.limit:
                start += self.limit
                sent = 0
                for result in self._queryiter('%s LIMIT %d %d' % (stmt, start, self.limit), *params):
                    sent += 1
                    yield result

    def _queryiter(self, stmt, *params):
        print stmt,params
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Executing statement %s" % stmt)
        cursor = MySQLdb.cursors.SSDictCursor(self.connection)
        if self.forceencoding:
            cursor.execute('SET NAMES %s;' % self.forceencoding)
            cursor.execute('SET CHARACTER SET %s;' % self.forceencoding)
            cursor.execute('SET character_set_connection=%s;' % self.forceencoding)
        try:
            cursor.execute(stmt, params or None)
            results = cursor.fetchmany(size=self.fetchsize)
            while results:
                for result in results:
                    yield result
                results = cursor.fetchmany(size=self.fetchsize)
            cursor.close()
        except MySQLdb.Error, exc:
            trace = sys.exc_info()[2]
            raise DataSourceError(
                'Query failed: Stmt: "%s". Params: "%s": %s' % (stmt, str(params), exc)
            ), None, trace



class URITemplate:
    """
    String Template wrapper useful to manage urls
    """
    def __init__(self, uri):
        self.uri = string.Template(uri)

    def bind(self, param):
        return self.uri.safe_substitute(param)

class SolrResultSet(object):
    """
    ResultSet used internally by datasource to retrieve records
    """
    def __init__(self):
        self.response = None

    def setresponse(self, response):
        self.response = response

    def numfound(self):
        return self.response['response']['numFound']

    def iterator(self):
        for doc in self.response['response']['docs']:
            yield doc
 
class SolrFacetResultSet(SolrResultSet):
    """
    Solr Facet iterator
    """
    def __init__(self, facet):
        super(SolrFacetResultSet, self).__init__()
        self.facet = facet

    def numfound(self):
        return len(self.response['facet_count']['facet_fields'][self.facet]) / 2

    def iterator(self):
        facetvalues = self.response['facetcount']['facet_fields'][self.facet]
        for i in range(0, len(facetvalues), 2):
            yield {self.facet: facetvalues[i],
                   'count': facetvalues[i+1]}

class SolrDataSource(DataSource):
    """
    Solr interface
    """
    def __init__(self, urltemplate, rows = 1000, timeout = None, 
                 maxrows = sys.maxint, resultiterator = None):
        super(SolrDataSource, self).__init__()
        self.uritemplate = URITemplate(urltemplate)
        self.timeout = int(timeout) if timeout else timeout
        self.rows = rows
        self.maxrows = maxrows
        self.resultiterator = resultiterator or SolrResultSet()

    def literal(self, value):
        return urllib.quote_plus(value)

    def bind(self, stmt, param, escape=True):
        return URITemplate(stmt).bind({'QUERY':self.literal(param) if escape else param})

    def query(self, stmt, *params):
        return [r for r in self.queryiter(stmt, *params)]
         
    def queryiter(self, stmt, *params):
        """Executes a solr query fetching the full resultset"""
        stmt = self.uritemplate.bind({'QUERY': stmt})
        logger.debug("Stmt after bind: %s" % stmt)
        uritemplate = URITemplate(stmt + '&rows=%d' % self.rows + \
                                  '&wt=json&start=$START')
        start = 0
        uri = uritemplate.bind({'START':start})
        logger.debug("Uri after bind: %s" % uri)
        self.resultiterator.setresponse(self._query(uri))
        total = min(self.resultiterator.numfound(), self.maxrows)
        done = 0
        for result in self.resultiterator.iterator():
            done += 1
            yield result

        while done < total:
            start += self.rows
            uri = uritemplate.bind({'START':start})
            self.resultiterator.setresponse(self._query(uri))
            for result in self.resultiterator.iterator():
                done += 1
                yield result

    def _query(self, uri):
        """Call a solr uri using json"""
        logger.info("Solr query: %s" % uri)
        if type(uri) == unicode:
            uri = uri.encode('utf-8')
        response = fetchers.fetch(uri + "&wt=json", timeout=self.timeout)
        if response.status != 200:
            raise DataSourceError(
                "Http call '%s' failed!: %s" % (uri, response.status)
            )
        data = None
        try:
            data = json.loads(response.body.decode('utf-8'))
        except Exception, ex:
            logger.warning("Exception parsing json response: %s" % response.body)
            logger.exception(ex)
        return data

    def connect(self):
        """Unused"""
        logger.debug("connecting to solr")

    def disconnect(self):
        """Unused"""
        pass

class SolrFacetDataSource(SolrDataSource):
     def __init__(self, urltemplate, facet, rows = 1000, timeout = None, 
                        maxrows = sys.maxint):
        super(SolrFacetDataSource, self).__init__(urltemplate, rows, timeout, 
                                                  maxrows, 
                                                  resultiterator = SolrFacetResultSet(facet))
 
class TestMySQLDataSource(unittest.TestCase):
    def setUp(self):
        pass

    def testQuery(self):
        dbparams = {}

    def tearDown(self):
        pass

class TestSolrDataSource(unittest.TestCase):
    def setUp(self):
        pass

    def testQuery(self):
        #template = "http://liqit-1-dbm:8080/solr/imgit2/select/?q=$QUERY&sort=DocDatetime%20desc"
        template = "http://google.com:8080/solr/imgit2/select/?q=$QUERY&sort=DocDatetime%20desc"
        datasource = SolrDataSource(template, rows = 5, timeout = 10)
        print datasource.query("Tags:ipad")
        datasource.disconnect()

    def tearDown(self):
        pass

def main():
    lformat = "[%(asctime)s] %(module)15s:%(name)10s:%(lineno)4d [%(levelname)6s]: %(message)s"
    logging.basicConfig(level=logging.INFO,
                        format=lformat)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSolrDataSource)
    #suite.addTest(TestSolrDataSource())
    unittest.TextTestRunner(verbosity=2).run(suite)

    #suite = unittest.TestLoader().loadTestsFromTestCase(TestSolrDataSource)

if __name__ == '__main__':
    main()

