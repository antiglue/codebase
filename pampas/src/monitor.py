


from pampas import AMQClientFactory
import time

# parametri di connessione ad ActiveMQ
amqparams = {'host_and_ports':[('localhost', 61116)]}
# coda di input dei messaggi
messagequeue = '/queue/social'

# istanzio la factory dei producer/consumer e setto i parametri
amqfactory = AMQClientFactory(amqparams)
amqfactory.setMessageQueue(messagequeue)

monitor = amqfactory.createConsumerClient()
with monitor:
    while True:
        print monitor.stats()
        time.sleep(1)
