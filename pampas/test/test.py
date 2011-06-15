

import pampas
import unittest
import time

def testf(headers, message):
    print "testf", message

class TestPipelineProcessor(unittest.TestCase):
    def setUp(self):
        self.amqparams = {'host_and_ports':[('localhost', 61116)]}
        self.destination = '/queue/test'
        self.factory = pampas.AMQClientFactory(self.amqparams)
        self.factory.setMessageQueue(self.destination)
        monitor = self.factory.createConsumerClient()
        monitor.connect()
        self.monitor = monitor
        #self.proc = pampas.PipelineProcessor('testpipe', './etc')

    def testFactory(self):
        expectedmessage = 20
        print "Spawning 3 workers"
        #procs = self.factory.spawnConsumers(self.proc, 3)
        procs = self.factory.spawnConsumers(testf, 3)
        time.sleep(3)
        print "Process spawned. Testing..."
        self.assertEqual(len(self.monitor.ping()), 3)

        print "Sending test messages..."
        with self.factory.createProducer() as producer:
            for i in range(expectedmessage):
                producer.sendMessage("test", headers={'url':'http://www.liquida.it/%d' % i})

        print "Messages sent, waiting for consumers"
        time.sleep(3)
        self.assertEqual(reduce(lambda tot, stat: tot + stat['received'], self.monitor.stats(), 0), expectedmessage)

        print "Stopping consumer"
        self.monitor.stopConsumers()
        time.sleep(3)
        self.assertEqual(len(self.monitor.ping()),  0)
        
    def testProcessing(self):
        #process = self.factory.spawnConsumers(
        pass

    def tearDown(self):
        self.factory.disconnectAll()

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPipelineProcessor)
    unittest.TextTestRunner(verbosity=2).run(suite)
