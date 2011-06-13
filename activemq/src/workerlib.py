# -*- encoding: utf-8 -*-

import stomp
import logging
import traceback
import string
import time
import hashlib
import random
import pickle
import sys
import os
import socket
import threading
import weakref
from functools import wraps
from pprint import pprint as pp
from pprint import pformat as pf
from multiprocessing import Process

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(module)15s:%(name)10s:%(lineno)4d [%(levelname)6s]:  %(message)s")
logger = logging.getLogger()


class Error(Exception):
    """
    Base class for exceptions fired by this module
    """
    pass

class ConnectionError(Error):
    """
    Base class for exceptions fired by this module
    """
    pass

class EventDispatcher(object):
    """
    Dirty simple event handling: Dispatcher
    A Dispatcher (or a subclass of Dispatcher) stores event handlers that are 'fired' when interesting things happen.

    Create a dispatcher:
    >>> d = Dispatcher()

    Create a handler for the event and subscribe it to the dispatcher
    to handle Event events.  A handler is a simple function or method that
    accepts the event as an argument:

    >>> def handler1(event): print event
    >>> d.add_handler(Event, handler1)

    Now dispatch a new event into the dispatcher, and see handler1 get
    fired:

    >>> d.fire(Event(foo='bar', data='dummy', used_by='the event handlers'))

    The handle_events decorator can be used to mark event handler methods and let the dispather discovering them:

    >>> class T(object):
    ...     @handle_events(EventA,EventB)
    ...     def handle(self, event):
    ...             print event
    >>> d.attach_listener(T())
    >>> d.fire(Event())
 
    """

    def __init__(self):
        self.handlers = {}

    def add_handler(self, event, handler):
        if not self.handlers.has_key(event):
            self.handlers[event] = []
        self.handlers[event].append(handler)

    def remove_handler(self, event, handler):
        if not self.handlers.has_key(event): return
        self.handlers[event].remove(handler)

    def attach_listener(self, listener):
        # iterating over class method to find and register annotated handlers
        for method in filter(callable, map(lambda x: not x.startswith('_') and getattr(listener,x), dir(listener))):
            if hasattr(method, 'events'):
                for event_type in method.events:
                    self.add_handler(event_type, method)
    
    def detach_listener(self, listener):
        for method in filter(callable, map(lambda x: not x.startswith('_') and getattr(listener,x), dir(listener))):
            if hasattr(method, 'events'):
                for event_type in method.events:
                    self.remove_handler(event_type, method)

    def fire(self, event, *args):
        event_type = type(event)
        if not event_type in self.handlers: return

        for handler in self.handlers[event_type]:
            try:
                handler(event, *args)
            except: # non ammesse eccezioni
                traceback.print_exc()

def handle_events(*event_types):
    """
    The handle_events decorator marks a target method as receiver
    of specified events and is designed to be used on listener objects. 
    A dispatcher can bind the decorated method to events using the attach_listener call
    """

    def _handle_event(targetmethod):
        @wraps(targetmethod)
        def handler(*args, **kwds):
            return targetmethod(*args, **kwds)
        handler.events = event_types
        return handler
    return _handle_event
        
class Event(object):
    """
    An event is a container for attributes.  The source of an event
    creates this object, or a subclass, gives it any kind of data that
    the events handlers need to handle the event, and then calls
    notify(event).

    The target of an event registers a function to handle the event it
    is interested with subscribe().  When a sources calls
    notify(event), each subscriber to that even will be called i no
    particular order.
    """

    def __init__(self, **kw):
        self.__dict__.update(kw)

    def __repr__(self):
        attrs = self.__dict__.keys()
        attrs.sort()
        return '<events.%s %s>' % (self.__class__.__name__, [a for a in attrs],)

class AmqErrorEvent(Event):
    """
    A AmqErrorEvent is fired by AMQListener when it receives an error 
    """
    pass

class StopWorkerEvent(Event):
    """
    A StopWorkerEvent is fired by AMQListener when it receives a stop command
    """
    pass

class MessageEvent(Event):
    """
    A MessageEvent is fired by AMQListener when it receives a message to elaborate
    """
    pass

class StatsEvent(Event):
    """
    A StatsEvent
    """
    pass

class PingEvent(Event):
    """
    A PingEvent
    """
    pass

class AMQClient(object):
    """
    An AMQClient handles basic stomp operations such as connect, send and disconnect.
    "with" statement is supported
    """

    def __init__(self, amqparams):
        self.connection = stomp.Connection(**amqparams)
        self.subscriptions = []
        self.cid = '%s.%s.%s' % (socket.gethostname(), os.getpid(), random.randrange(200))

    def connect(self):
        self.cid = '%s.%s.%s' % (socket.gethostname(), os.getpid(), random.randrange(200))
        logger.debug("Connecting %s" % self.cid)
        try:
            self.connection.start()
            self.connection.connect(wait=True)
            logger.debug("Connected.")
        except stomp.exception.ReconnectFailedException, e:
            logger.warning("Connection error")
            trace = sys.exc_info()[2]
            raise ConnectionError('Connection failed!'), None, trace

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            if self.subscriptions:
                for subscr in self.subscriptions:
                    self.unsubscribe(subscr)

            self.connection.disconnect()

    def subscribe(self, destination, params = None, ack = 'client'):
        logger.debug("Subscribing to %s" % destination)
        self.subscriptions.append(destination)
        params = params or {}
        params['id'] = self.cid
        self.connection.subscribe(params, destination=destination, ack=ack)

    def unsubscribe(self, destination):
        logger.debug("Unsubscribing to %s" % destination)
        self.subscriptions.remove(destination)
        self.connection.unsubscribe(id=self.cid, destination=destination)
        
    def send(self, destination, message, headers = None, ack = 'client'):
        try:
            headers = headers or {}
            if not 'id' in headers:
                headers['id'] = self.cid
            self.connection.send(message=message, headers=headers, 
                                 destination=destination, ack=ack)
        except stomp.exception.NotConnectedException, ex:
            trace = sys.exc_info()[2]
            raise ConnectionError('Connection failed!'), None, trace

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, tb):
        self.disconnect()

    def __del__(self):
        self.disconnect()

class BaseConsumer(AMQClient):
    """
    Basic Consumer class. Wait for incoming message on a destination. Forwards received messages on Events
    """
    COMMAND_HEADER = 'wl-cmd'
    SLEEP_EXIT = 0.1

    def __init__(self, amqparams, destination, params, ackmode):
        AMQClient.__init__(self, amqparams)
        self.can_run = False
        self.listener = BaseConsumer.AMQListener(self.connection)
        self.listener.attach_listener(self)
        self.connection.set_listener('consumer', self.listener)
        self.subscriptionparams = (destination, params, ackmode)

    def connect(self):
        AMQClient.connect(self)
        self.can_run = self.connection.is_connected()
        dest, params, ackmode = self.subscriptionparams
        self.subscribe(destination=dest, params=params, ack=ackmode)
        return True

    def disconnect(self):
        AMQClient.disconnect(self)
        self.can_run = False

    def run(self):
        while self.can_run:
            try: 
                time.sleep(BaseConsumer.SLEEP_EXIT)
            except KeyboardInterrupt:
                self.disconnect()


    class AMQListener(stomp.ConnectionListener):
        """
        Stomp bridging layer: fires received messages as events
        """

        DEBUG_MESSAGE = False
        def __init__(self, connection):
            self.connection = connection
            self.dispatcher = EventDispatcher()
        
        def attach_listener(self, listener):
            self.dispatcher.attach_listener(listener)

        def on_error(self, headers, message):
            logger.warning('received an error %s' % message)
            self.dispatcher.fire(AmqErrorEvent(headers=headers, message=message)) 

        def on_receipt(self, headers, message):
            logger.debug("RECEIPT %s %s" % (headers, message))

        def on_message(self, headers, message):
            logger.debug("Received: %s" % str((headers, message)))
            event = MessageEvent
            if BaseConsumer.COMMAND_HEADER in headers:
                logger.debug('Got %s COMMAND' % message)
                event = globals().get(headers[Consumer.COMMAND_HEADER], None)
                assert event

            if self.DEBUG_MESSAGE and logger.isEnabledFor(logging.DEBUG):
                logger.debug("Received message:")
                for k,v in headers.iteritems():
                    logger.debug('header: key %s , value %s' %(k,v))
                logger.debug('body: %s'% message)

            self.dispatcher.fire(event(headers=headers, message=message))

class StatsConsumer(BaseConsumer):
    """
    A StatsConsumer adds statistics to BaseCosumer
    """

    def __init__(self, amqparams, destination, params, ackmode):
        BaseConsumer.__init__(self, amqparams, destination, params, ackmode)
        self.stats = Consumer.Stats(self)
        self.listener.attach_listener(self.stats)

    class Stats(object):
        def __init__(self, owner):
            self.owner = owner
            self.received = 0
            self.time = 0
            self.docs_s = 0
            self.lastprocessed = None
            self.lastobserv = {'time':time.time(),'received':0}
            self.observdelay = 5
            self.idledelay = 3

        @handle_events(MessageEvent)
        def onMessageEvent(self, event):
            self.time = time.time()
            self.lastprocessed = event.headers['message-id']
            self.received += 1
            if self.time - self.lastobserv['time'] > self.observdelay:
                self.docs_s = (self.received - self.lastobserv['received']) / (self.time - self.lastobserv['time'])
                self.lastobserv = {'time':self.time, 'received':self.received}

        def getdata(self):
            return {'id': self.owner.cid,
                    'received':self.received,
                    'time': self.time,
                    'status': 'idle' if time.time() - self.time > self.idledelay else 'working',
                    'docs/s': self.docs_s,
                    'lastprocessed': self.lastprocessed}


class Consumer(StatsConsumer):
    """
    A Consumer is a process waiting for incoming messages on a queue and listen for commands on a topic.
    It terminates when a stop command is received. AMQClientfactory.stopConsumers can be used to broadcast shutdown messages.

    >>> def p(h,m): print h.m
    >>> c = Consumer({'host_and_ports':[('localhost', 61116)]}, p, {'/queue/social',{},'auto'}, {'/topic/cmd',{},'auto'} )
    >>> c.connect()
    >>> c.run()
    """ 

    def __init__(self, amqparams, processor, subscriptionparams, commandtopicparams):
        StatsConsumer.__init__(self, amqparams, *subscriptionparams)
        self.processor = processor
        self.controller = Consumer.Controller(self, amqparams, (commandtopicparams[0], commandtopicparams[1], 'auto'))
        self.controllerthread = threading.Thread(target=self.startController)
        #self.subscriptions = { 'input':(inputqueue, inputparams or {'activemq.priority':0, 'activemq.prefetchSize':1}, 'client'),
        #                       'cmd': (commandtopic, {'activemq.priority':10}, 'auto') }

    def startController(self):
        with self.controller:
            self.controller.run()

    def connect(self):
        self.controllerthread.start()
        StatsConsumer.connect(self)

    def disconnect(self):
        self.controller.disconnect()
        if self.controllerthread.is_alive():
            self.controllerthread.join()
        StatsConsumer.disconnect(self)

    @staticmethod
    def pingMessage():
        return ({BaseConsumer.COMMAND_HEADER:'PingEvent'},'ping')

    @staticmethod
    def stopMessage():
        return ({BaseConsumer.COMMAND_HEADER:'StopWorkerEvent'},'stop')

    @staticmethod
    def statsMessage():
        return ({BaseConsumer.COMMAND_HEADER:'StatsEvent'},'stats')


    @handle_events(MessageEvent)
    def onMessageEvent(self, event):
        logger.debug("CID %s:Received message: %s" % (self.cid, event))
        try:
            self.processor(event.headers, event.message)
            self.connection.ack(event.headers)
        except Exception as ex:
            logger.warning("Catched processor exception:")
            logger.exception(ex)

    class Controller(BaseConsumer):
        def __init__(self, owner, amqparams, subscriptionparams):
            BaseConsumer.__init__(self, amqparams, *subscriptionparams)
            self.owner = owner

        @handle_events(AmqErrorEvent)
        def onAmqErrorEvent(self, event):
            logger.debug("received error: %s" % event)
            self.owner.disconnect()

        @handle_events(StatsEvent)
        def onStatsEvent(self, event):
            logger.debug("received stats request: %s" % event)
            self.send(message=pickle.dumps(self.owner.stats.getdata()), headers={'correlation-id':event.headers['correlation-id'], 
                                                                                 self.COMMAND_HEADER:'StatsEvent'},
                      destination=event.headers['reply-to'], ack='auto')

        @handle_events(PingEvent)
        def onPingEvent(self, event):
            logger.debug("received ping request: %s" % event)
            self.send(message=pickle.dumps({'pong':self.owner.cid}), headers={'correlation-id':event.headers['correlation-id'], 
                                                                         self.COMMAND_HEADER:'PingEvent'},
                      destination=event.headers['reply-to'], ack='auto')

        @handle_events(StopWorkerEvent)
        def onStopWorkerEvent(self, event):
            logger.debug("received stop: %s" % event)
            self.disconnect()
            self.owner.disconnect()

class ConsumerClient(AMQClient):
    DEFAULT_TIMEOUT = 3
    def __init__(self, amqparams, commandtopic):
        AMQClient.__init__(self, amqparams)
        self.commandtopic = commandtopic
        self.cid = hashlib.md5(str(time.time() * random.random())).hexdigest()
        self.replyqueue = '/temp-queue/%s' % self.cid
        self.counter = 0
        self.resultholder = {}

    def connect(self):
        AMQClient.connect(self)
        self.connection.set_listener(self.cid, self)
        self.connection.subscribe(destination=self.replyqueue, ack='auto')

    def disconnect(self):
        if self.connection.is_connected():
            self.connection.unsubscribe(destination=self.replyqueue)
        AMQClient.disconnect(self)

    def stopConsumers(self):
        headers, message = Consumer.stopMessage()
        self.send(message=message, headers=headers,
                  destination=self.commandtopic, ack='auto')

    def on_message(self, headers, message):
        stats = pickle.loads(message)
        logger.info("Client %s Received response from: %s - %s" % (self.cid, str(stats), headers['id']))
        self.resultholder.setdefault(headers[Consumer.COMMAND_HEADER], {}).setdefault(headers['correlation-id'], []).append(stats)

    def ping(self, timeout = DEFAULT_TIMEOUT, expectedcount = -1):
        return self.execute('PingEvent', timeout, expectedcount)

    def stats(self, timeout = DEFAULT_TIMEOUT, expectedcount = -1):
        return self.execute('StatsEvent', timeout, expectedcount)

    def execute(self, cmd, timeout, expectedcount):
        headers, message = getattr(Consumer, '%sMessage' % cmd.replace('Event','').lower())()
        correlationid = '%s.%d' % (self.cid, self.counter)
        headers.update({'reply-to': self.replyqueue,
                        'correlation-id': correlationid,
                        'priority':127 })
        self.resultholder.setdefault(cmd,{})[correlationid] = []
        logger.info("Sending %s request to: %s" % (cmd, self.commandtopic))
        try:
            self.send(message=message, headers=headers,
                      destination=self.commandtopic, ack='auto')
            self.counter += 1
            if expectedcount > 0:
                while len(self.resultholder[cmd][correlationid]) < expectedcount:
                    time.sleep(1)
            else:
                time.sleep(timeout)
            ret = self.resultholder[cmd].pop(correlationid)
            return ret
        except KeyboardInterrupt:
            raise
        except:
            traceback.print_exc()
            return []

    
class Producer(AMQClient):
    def __init__(self, amqparams, destination, defaultheaders = None):
        AMQClient.__init__(self, amqparams)
        self.defaultheaders = defaultheaders or {}
        self.destination = destination

    def sendMessage(self, message, headers = None):
        hs = dict( self.defaultheaders.items() + 
                   (headers or {}).items() )
        self.send(message=message, headers=hs,
                  destination=self.destination, ack='client')

class AMQClientFactory:
    """
    A AMQClientFactory should be used to inizialize amq clients
    """

    def __init__(self, amqparams):
        self.params = {}
        self.params['amqparams'] = amqparams
        self.references = weakref.WeakValueDictionary()

    def setMessageQueue(self, messagequeue):
        self.params['messagequeue'] = messagequeue
        if 'commandtopic' not in self.params:
            self.params['commandtopic'] = '/topic/%s_cmd' % messagequeue.split('/').pop()
            print self.params['commandtopic']

    def setCommandTopic(self, commandtopic):
        self.params['commandtopic'] = commandtopic

    def createConsumerClient(self, commandtopic = None):
        if not commandtopic and not 'commandtopic' in self.params:
            raise NameError("Cannot create consumer monitor. No command queue set! Please set the commandtopic argument or call setCommandTopic before")
        obj = ConsumerClient(self.params['amqparams'], commandtopic or self.params['commandtopic'])
        self.references[id(obj)] = obj
        return obj

    def createProducer(self, messagequeue = None, defaultheaders = None):
        if not messagequeue and not 'messagequeue' in self.params:
            raise NameError("Cannot create producer. No message queue set! Please set the messagequeue argument or call setMessageQueue before")
        obj = Producer(self.params['amqparams'], messagequeue or self.params['messagequeue'], defaultheaders)
        self.references[id(obj)] = obj
        return obj


    def createConsumer(self, acallable, messagequeue = None, commandtopic = None):
        if not messagequeue and not 'messagequeue' in self.params:
            raise NameError("Cannot create consumer. No message queue set! Please set the messagequeue argument or call setMessageQueue before")

        if not commandtopic and not 'commandtopic' in self.params:
            raise NameError("Cannot create consumer. No command queue set! Please set the commandtopic argument or call setCommandTopic before")

        obj = Consumer(self.params['amqparams'], 
                       acallable, 
                       (messagequeue or self.params['messagequeue'], {'activemq.priority':0, 'activemq.prefetchSize':1}, 'client'), 
                       (commandtopic or self.params['commandtopic'], {'activemq.priority':10}, 'auto')
                      )
        self.references[id(obj)] = obj
        return obj

    def spawnConsumers(self, f, consumercount):
        def startConsumer(c, i):
            def _startConsumer():
                logger.debug("Creating consumer %d" % i)
                c.connect()
                c.run()
            return _startConsumer

        processes = [ Process(target=startConsumer(self.createConsumer(f), i))  for i in range(consumercount)]
        # avvio i processi consumer
        logger.debug("Starting consumers")
        for p in processes:
            p.start()
        logger.debug("Consumers started")
        return processes

    def disconnectAll(self):
        for o in self.references.values():
            if o is not None:
                o.disconnect()

def main():
    # parametri di connessione ad ActiveMQ
    amqparams = {'host_and_ports':[('localhost', 61116)]}
    # coda di input dei messaggi
    messagequeue = '/queue/social'

    # funzione da eseguire per ogni messaggio
    def f(h,m):
        logger.info("Processor::MessageEvent: %s - %s" % (str(h),str(m)))
        time.sleep(2)

    # istanzio la factory dei producer/consumer e setto i parametri
    amqfactory = AMQClientFactory(amqparams)
    try:
        amqfactory.setMessageQueue(messagequeue)

        # istanzio il monitor per statistiche e controllo
        monitor = amqfactory.createConsumerClient()
        monitor.connect()

        processes = amqfactory.spawnConsumers(f, 3)
        # per avviare processo singolo:
        # c = amqfactory.createConsumer(f)
        # c.connect()
        # c.run()

        # creo ed uso il producer:
        # versione standard:
        """
        producer = amqfactory.createProducer()
        producer.connect()
        for i in range(10):
            producer.sendMessage("ciao")
        producer.disconnect()
        """

        time.sleep(3)
        logger.info("PING:")
        logger.info(pf(monitor.ping()))
        # versione with(non necessita di connect e di disconnect:
        with amqfactory.createProducer() as producer:
            for i in range(40):
               producer.sendMessage("messaggio%d" % i)
 
        logger.info(pf(monitor.ping()))

        logger.info("Sleeping 5 seconds before stopping workers!")
        for i in range(5):
            time.sleep(1)
            pp(monitor.stats(timeout=3))
            
        logger.info("Sending stop command")
        monitor.stopConsumers()

        logger.debug("Stopping consumers")
        for p in processes:
            p.join()
        logger.debug("Consumers stopped")

        monitor.disconnect()
        logger.debug("Run finished")
    except:
        traceback.print_exc()
        amqfactory.disconnectAll()

    print "fine main"
    

if __name__ == '__main__':
    main()


