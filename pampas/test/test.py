

import pampas
import unittest
import time
import logging

logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] %(module)15s:%(name)10s:%(lineno)4d [%(levelname)6s]:  %(message)s")

def testf(headers, message):
    print "testf", message

class TestPipelineProcessor(unittest.TestCase):
    def setUp(self):
        self.amqparams = {'host_and_ports':[('localhost', 61116)]}
        self.destination = '/queue/test'
        self.factory = pampas.AMQClientFactory(self.amqparams)
        self.factory.setMessageQueue(self.destination)
        #self.proc = pampas.PipelineProcessor('testpipe', './etc')
        monitor = self.factory.createConsumerClient()
        monitor.connect()
        self.monitor = monitor
        self.procs = []

    def testASpawnProcess(self):
        print "Spawning 3 workers"
        self.procs = self.factory.spawnConsumers(testf, 3)
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            self.factory.disconnectAll()
            raise SystemExit()

        print "Process spawned. Testing..."
        self.assertEqual(len(self.monitor.ping()), 3)

    def testEncoder(self):
        data = {'1':1,'2':2}
        je = JSONEncoder()
        encoded = je.encode(data)
        dec = je.decode(encoded)
        self.assertTrue(isinstance(dec, dict))
        self.assertEqual(data, dec)

    def testProducer(self):
        expectedmessage = 20
        print "Sending test messages..."
        with self.factory.createProducer() as producer:
            for i in range(expectedmessage):
                producer.sendMessage("test%d" % i, headers={'url':'http://www.liquida.it/%d' % i})

        print "Messages sent, waiting for consumers"
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            self.factory.disconnectAll()
            raise SystemExit()
        self.assertEqual(reduce(lambda tot, stat: tot + stat['received'], self.monitor.stats(), 0), expectedmessage)

    def testProducerBatch(self):
        expectedmessage = 20
        print "Sending test messages..."
        with self.factory.createBufferedProducer(15) as producer:
            for i in range(expectedmessage):
                producer.sendMessage("test%d" % i, headers={'url':'http://www.liquida.it/%d' % i})

        print "Messages sent, waiting for consumers"
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            self.factory.disconnectAll()
            raise SystemExit()
        self.assertEqual(reduce(lambda tot, stat: tot + stat['received'], self.monitor.stats(), 0), expectedmessage + 20)

    def testZStopConsumers(self):
        print "Stopping consumer"
        self.monitor.stopConsumers()
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            self.factory.disconnectAll()
            raise SystemExit()

        self.assertEqual(len(self.monitor.ping()),  0)
        
    def tearDown(self):
        self.factory.disconnectAll()

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPipelineProcessor)
    unittest.TextTestRunner(verbosity=2).run(suite)
