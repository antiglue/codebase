#!/usr/bin/env python2.7
# -*- encoding: utf-8 -*-

import hashlib
import logging
import os
import pickle
import random
import socket
import stomp
import sys
import threading
import time
import traceback
import weakref
import json
import base64
from pprint import pprint as pp
from pprint import pformat as pf
from events import EventDispatcher, Event, handle_events
from multiprocessing import Process

#logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(module)15s:%(name)10s:%(lineno)4d [%(levelname)6s]:  %(message)s")
#logging.config.fileConfig(os.path.join('etc', 'logging.conf'))
logger = logging.getLogger('pampas')


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

class MessageEvent(Event):
    """
    A MessageEvent is fired by AMQListener when it receives a message to elaborate
    """
    pass

class MessageProcessedEvent(Event):
    """
    A MessageProcessedEvent is fired by AMQListener at the end 
    """
    pass

class MessageProcessingErrorEvent(Event):
    """
    A MessageEvent is fired by AMQListener when message processing fails
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

class DummyEncoder(object):
    def encode(self, data):
        return data

    def decode(self, data):
        return data

class JSONEncoder(object):
    def encode(self, data):
        return base64.b64encode(json.dumps(data))

    def decode(self, data):
        try:
            return json.loads(base64.b64decode(data))
        except TypeError, e:
            logger.warning("Message decoding error: %s" % str(e))
            logger.exception(e)

COMMAND_HEADER = 'wl-cmd'

class AMQStompConnector(object):
    """
    An AMQStompConnector handles basic stomp operations such as connect, send and disconnect.
    "with" statement is supported
    """

    COMMAND_HEADER = 'wl-cmd'

    def __init__(self, cid, amqparams, encoder):
        self.cid = cid
        self.connection = stomp.Connection(**amqparams)
        self.encoder = encoder
        self.listener = AMQStompConnector.AMQListener(self.connection, self.encoder)
        self.connection.set_listener(self.cid, self.listener)
        self.subscriptions = []

    def connect(self):
        #self.cid = '%s.%s.%s' % (socket.gethostname(), os.getpid(), random.randrange(200))
        logger.debug("Connecting %s" % self.cid)
        try:
            self.connection.start()
            self.connection.connect(wait=True)
            logger.debug("Connected.")
        except stomp.exception.ReconnectFailedException:
            logger.critical("Connection error")
            trace = sys.exc_info()[2]
            raise ConnectionError('Connection failed!'), None, trace

    def is_connected(self):
        return self.connection and self.connection.is_connected()

    def disconnect(self):
        if self.is_connected():
            if self.subscriptions:
                for subscr in self.subscriptions:
                    self.unsubscribe(subscr)
            self.connection.disconnect()

    def ack(self, headers = None):
        self.connection.ack(headers=headers or {})

    def add_listener(self, listener):
        self.listener.attach_listener(listener)

    def subscribe(self, destination, params = None, ack = 'client'):
        logger.debug("Subscribing to %s" % destination)
        self.subscriptions.append(destination)
        params = params or {'id': self.cid}
        self.connection.subscribe(params, destination=destination, ack=ack)

    def unsubscribe(self, destination):
        logger.debug("Unsubscribing to %s" % destination)
        self.subscriptions.remove(destination)
        self.connection.unsubscribe(id=self.cid, destination=destination)
        
    def send(self, destination, message, headers = None, ack = 'client'):
        try:
            headers = headers or {}
            headers['id'] = self.cid
            self.connection.send(message=self.encoder.encode(message), headers=headers, 
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

    class AMQListener(stomp.ConnectionListener):
        """
        Stomp bridging layer: fires received messages as events
        """

        DEBUG_MESSAGE = False
        def __init__(self, connection, encoder):
            self.connection = connection
            self.encoder = encoder
            self.dispatcher = EventDispatcher()
            self.messagelock = threading.Lock()
        
        def attach_listener(self, listener):
            self.dispatcher.attach_listener(listener)

        def on_error(self, headers, message):
            logger.warning('received an error %s' % message)
            self.dispatcher.fire(AmqErrorEvent(headers=headers, message=message)) 

        def on_receipt(self, headers, message):
            logger.debug("RECEIPT %s %s" % (headers, message))

        def on_message(self, headers, message):
            message = self.encoder.decode(message)
            logger.debug("Received: %s" % str((headers, message)))
            with self.messagelock:
                event = MessageEvent
                if COMMAND_HEADER in headers:
                    logger.debug('Got %s COMMAND' % message)
                    event = globals().get(headers[COMMAND_HEADER], None)
                    assert event

                if self.DEBUG_MESSAGE and logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Received message:")
                    for k,v in headers.iteritems():
                        logger.debug('header: key %s , value %s' %(k,v))
                    logger.debug('body: %s'% message)

                logger.debug("Firing event: %s" % str(event))
                if 'buffersize' in headers:
                    for m in pickle.loads(message):
                        self.dispatcher.fire(event(headers=headers, message=m))
                else:
                    self.dispatcher.fire(event(headers=headers, message=message))

                self.dispatcher.fire(MessageProcessedEvent(headers=headers, message=message))

class ErrorStrategy(object):
    """
    An ErrorStrategy defines the chain of actions a consumer will run to handle errors
    """
    def __init__(self):
        self.actionchain = {}

    def append(self, eventType, action):
        self.actionchain.setdefault(eventType, []).append(action)
        return self

    def __call__(self, event):
        logger.debug("Received error: %s" % type(event))
        try:
            for action in self.actionchain.get(type(event), []):
                action(event)
        except:
            traceback.print_exc() # error handler should not fail

    def __repr__(self):
        return 'ErrorStrategy - chain: %s' % '\n\t'.join(('on "%s": %s' % (k.__name__, '=>'.join([ v.__class__.__name__ for v in vs])) for k,vs in self.actionchain.items()))

class ErrorLogStrategy(ErrorStrategy):
    def __init__(self, level = logging.WARNING, logstacktrace = True, logfullmessage = False):
        self.level = level
        self.logfullmessage = logfullmessage
        self.logstacktrace = logstacktrace

    def __call__(self, event):
        logger.log(self.level, '%s: "%s" => Message={headers:"%s", message="%s"}' % 
                                (str(event.exc_cause.__class__.__name__), 
                                 event.exc_description,
                                 str(event.headers), 
                                 str(event.message) if self.logfullmessage else '...'))
        if self.logstacktrace:
            logger.exception(event.exc_cause)

class ErrorDLQStrategy(ErrorStrategy):
    def __init__(self, destination, defaultheaders = None):
        self.destination = destination
        self.defaultheaders = defaultheaders or {}
 
    def __call__(self, event):
        headers = self.defaultheaders.copy()
        headers.update({'errorType': str(event.exc_cause), 
                        'errorDesc': event.exc_description})
        headers.update(event.headers)
        if 'destination' in headers:
            del headers['destination']
        event.owner.connector.send(message=event.message, headers=headers, 
                                   destination=self.destination, ack='auto')

class ErrorStopStrategy(ErrorStrategy):
    def __call__(self, event):
        self.owner.disconnect()

class ErrorFailStrategy(ErrorStrategy):
    def __call__(self, event):
        raise event.exc_cause

class ErrorUserFunctStrategy(ErrorStrategy):
    def __init__(self, callback):
        self.callback = callback

    def __call__(self, event):
        self.callback(event)

try:
    from nagiosLib.nagiosClass import writeNagiosException
    class NagiosErrorStrategy(ErrorStrategy):
        PROJECTNAME = 'pampas'
        def __call__(self, event):
            writeNagiosException(serviceName=self.PROJECTNAME, 
                                 errorToWrite='%s - %s' % (str(event.exc_cause.__class__.__name__), event.exc_description))
except:
    class NagiosErrorStrategy(ErrorStrategy):
        PROJECTNAME = 'pampas'
        def __call__(self, event):
            raise ImportError("nagiosLib unavailable")

class BaseConsumer(object):
    """
    A basic Consumer class that listen for incoming message on a destination. 
    Received messages are dispatched to methods as events
    """
    SLEEP_EXIT = 0.1

    def __init__(self, connector, errorstrategy, destination, params, ackmode):
        self.connector = connector
        self.can_run = False
        self.connector.add_listener(self)
        self.subscriptionparams = (destination, params, ackmode)
        self.ackneeded = ackmode == 'client'
        self.errorstrategy = errorstrategy

    def set_error_strategy(self, errorstrategy):
        self.errorstrategy = errorstrategy

    def append_error_handler(self, strategy):
        self.errorstrategy.append(MessageProcessingErrorEvent, strategy)

    def handle_error(self, event):
        self.errorstrategy(event)

    def connect(self):
        self.connector.connect()
        self.can_run = self.connector.is_connected()
        dest, params, ackmode = self.subscriptionparams
        self.connector.subscribe(destination=dest, params=params, ack=ackmode)
        return True

    def disconnect(self):
        self.connector.disconnect()
        self.can_run = False

    def ack(self, headers = None):
        self.connector.ack(headers=headers or {})

    def run(self):
        while self.can_run:
            try: 
                time.sleep(BaseConsumer.SLEEP_EXIT)
            except KeyboardInterrupt:
                self.disconnect()
                raise SystemExit()

    @handle_events(MessageProcessedEvent)
    def on_message_processed(self, event):
        if self.ackneeded:
            self.ack(event.headers)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, tb):
        self.disconnect()

class StatsConsumer(BaseConsumer):
    """
    A StatsConsumer adds statistics and performance data to BaseCosumer.
    Statistics are calculated intercepting incoming messages.
    """

    def __init__(self, connector, errorstrategy,  destination, params, ackmode):
        BaseConsumer.__init__(self, connector, errorstrategy, destination, params, ackmode)
        self.stats = Consumer.StatsDelegate(self)
        self.connector.add_listener(self.stats)

    class StatsDelegate(object):
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
        def on_message_event(self, event):
            self.time = time.time()
            self.lastprocessed = event.headers['message-id']
            self.received += 1
            if self.time - self.lastobserv['time'] > self.observdelay:
                self.docs_s = (self.received - self.lastobserv['received']) / (self.time - self.lastobserv['time'])
                self.lastobserv = {'time':self.time, 'received':self.received}

        def getdata(self):
            return {'id': self.owner.connector.cid,
                    'received':self.received,
                    'time': self.time,
                    'status': 'idle' if time.time() - self.time > self.idledelay else 'working',
                    'docs/s': self.docs_s,
                    'lastprocessed': self.lastprocessed}


class Consumer(StatsConsumer):
    """
    A Consumer is a process waiting for incoming messages on a queue and 
    is listening for commands on a topic.
    Received messages are forwarded to the "process" callback function to be processed
    Supported Commands:
    - ping
    - stats
    - stop

    >>> c = Consumer({'host_and_ports':[('localhost', 61116)]}, f, {'/queue/social',{},'auto'}, {'/topic/social_cmd',{},'auto'} )
    >>> c.connect()
    >>> c.run()
    >>> c.disconnect()

    Commands can be sent using ConsumerClient instances:

    >>> def f(h,m): print h,m
    >>> amqfactory = AMQClientFactory({'host_and_ports':[('localhost', 61116)]})
    >>> amqfactory.spawnConsumers(f, 3)
    >>> client = amqfactory.createConsumerClient()
    >>> client.connect()
    >>> time.sleep(3)
    >>> assert len(client.ping()) == 3
    >>> client.stopConsumers()
    >>> assert len(client.ping()) == 0
    >>> client.disconnect()
 
    The consumer process will terminate if a stop command is received. 
    """ 

    def __init__(self, connector, controllerconnector, errorstrategy, processor, subscriptionparams, commandtopicparams):
        StatsConsumer.__init__(self, connector, errorstrategy, *subscriptionparams)
        self.processor = processor
        self.controller = Consumer.ControllerDelegate(self, controllerconnector, (commandtopicparams[0], commandtopicparams[1], 'auto'))
        self.controllerthread = threading.Thread(target=self.start_controller)

    def start_controller(self):
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
    def pingmessage():
        return ({COMMAND_HEADER:'PingEvent'},'ping')

    @staticmethod
    def stopmessage():
        return ({COMMAND_HEADER:'StopWorkerEvent'},'stop')

    @staticmethod
    def statsmessage():
        return ({COMMAND_HEADER:'StatsEvent'},'stats')

    @handle_events(MessageEvent)
    def on_message_event(self, event):
        logger.debug("CID %s:Received message: %s" % (self.connector.cid, event))
        try:
            self.processor(event.headers, event.message)
        except Exception as ex:
            self.handle_error(
                MessageProcessingErrorEvent(owner=self, 
                                            exc_cause=ex, 
                                            exc_description="Catched processor exception", 
                                            headers=event.headers, message=event.message)
            )
            #logger.warning("Catched processor exception:")
            #logger.exception(ex)

    class ControllerDelegate(BaseConsumer):
        def __init__(self, owner, connector, subscriptionparams):
            BaseConsumer.__init__(self, connector, ErrorStrategy(), *subscriptionparams)
            self.owner = owner

        @handle_events(AmqErrorEvent)
        def on_amqerror_event(self, event):
            logger.debug("received error: %s" % event)
            self.owner.disconnect()

        @handle_events(StatsEvent)
        def on_stats_event(self, event):
            logger.debug("received stats request: %s" % event)
            self.connector.send(message=self.owner.stats.getdata(), 
                                headers={'correlation-id':event.headers['correlation-id'], 
                                         COMMAND_HEADER:'StatsEvent'},
                                destination=event.headers['reply-to'], ack='auto')

        @handle_events(PingEvent)
        def on_ping_event(self, event):
            logger.debug("received ping request: %s" % event)
            self.connector.send(message={'pong':self.connector.cid}, headers={'correlation-id':event.headers['correlation-id'], 
                                                                          COMMAND_HEADER:'PingEvent'},
                      destination=event.headers['reply-to'], ack='auto')

        @handle_events(StopWorkerEvent)
        def on_stopworker_event(self, event):
            logger.debug("received stop: %s" % event)
            self.disconnect()
            self.owner.disconnect()

class ConsumerClient(object):
    """
    A ConsumerClient is responsible for communicating with Consumer via command topic.
    It is able to:
    - ping consumers
    - query consumers about statistics
    - stop consumers
    Actaually it only support broadcasting, so every consumer will receive the command
    """

    DEFAULT_TIMEOUT = 3
    def __init__(self, connector, commandtopic):
        self.connector = connector
        self.commandtopic = commandtopic
        self.cid = 'client-' + hashlib.md5(str(time.time() * random.random())).hexdigest()
        self.replyqueue = '/temp-queue/%s' % self.connector.cid
        self.counter = 0
        self.resultholder = {}

    def connect(self):
        self.connector.add_listener(self)
        self.connector.connect()
        self.connector.subscribe(destination=self.replyqueue, ack='auto')

    def disconnect(self):
        if self.connector.is_connected():
            self.connector.unsubscribe(destination=self.replyqueue)
        self.connector.disconnect()

    def stopConsumers(self):
        headers, message = Consumer.stopmessage()
        self.connector.send(message=message, headers=headers,
                  destination=self.commandtopic, ack='auto')

    def ping(self, timeout = DEFAULT_TIMEOUT, expectedcount = -1):
        return self.send_receive(Consumer.pingmessage(), timeout, expectedcount)

    def stats(self, timeout = DEFAULT_TIMEOUT, expectedcount = -1):
        return self.send_receive(Consumer.statsmessage(), timeout, expectedcount)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, tb):
        self.disconnect()

    def send_receive(self, cmdmsg, timeout, expectedcount):
        headers, message = cmdmsg
        cmd = headers[COMMAND_HEADER]
        correlationid = '%s.%d' % (self.connector.cid, self.counter)
        headers.update({'reply-to': self.replyqueue,
                        'correlation-id': correlationid,
                        'priority':127 })
        self.resultholder.setdefault(cmd,{})[correlationid] = []
        logger.debug("Sending %s request to: %s" % (cmd, self.commandtopic))
        try:
            self.connector.send(message=message, headers=headers,
                      destination=self.commandtopic, ack='auto')
            self.counter += 1
            if expectedcount > 0:
                while len(self.resultholder[cmd][correlationid]) < expectedcount and timeout > 0:
                    try:
                        time.sleep(1)
                        timeout -= 1
                    except KeyboardInterrupt:
                        raise SystemExit()
            else:
                try:
                    time.sleep(timeout)
                except KeyboardInterrupt:
                    raise SystemExit()

            ret = self.resultholder[cmd].pop(correlationid)
            return ret
        except KeyboardInterrupt:
            raise
        except:
            traceback.print_exc()
            return []

    @handle_events(MessageEvent,PingEvent,StatsEvent)
    def on_message(self, event):
        print event, event.headers, event.message
        headers, message = event.headers, event.message
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Client %s Received response from: %s - %s" % (self.connector.cid, str(message), headers['id']))
        logger.info("Client %s Received response from: %s - %s" % (self.connector.cid, str(message), headers['id']))

        cmd = headers[COMMAND_HEADER]
        correlationid = headers['correlation-id']
        if cmd in self.resultholder and correlationid in self.resultholder[cmd]:
            self.resultholder[cmd][correlationid].append(message)

   
class Producer(object):
    def __init__(self, connector, destination, defaultheaders = None):
        self.connector = connector
        self.defaultheaders = defaultheaders or {'persistent':'true'}
        self.destination = destination

    def connect(self):
        self.connector.connect()

    def disconnect(self):
        self.connector.disconnect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, tb):
        self.disconnect()

    def sendMessage(self, message, headers = None):
        hs = dict( self.defaultheaders.items() + 
                   (headers or {}).items() )
        self.connector.send(message=message, headers=hs,
                            destination=self.destination, ack='client')

class DynamicProxy(object):
    def __init__(self, target):
        self.real = target

    def __getattr__(self, name):
        return getattr(self.real, name)

class BufferedProducer(DynamicProxy):
    def __init__(self, producer, buffersize):
        DynamicProxy.__init__(self, producer)
        self.buffersize = buffersize
        self.buffer = []

    def sendMessage(self, message, headers = None):
        self.buffer.append((message, headers))
        if len(self.buffer) >= self.buffersize:
            batchmessage = pickle.dumps(self.buffer)
            self.real.sendMessage(batchmessage, headers = {'buffersize':len(self.buffer)})
            del self.buffer[:]

    def flush(self):
        if self.buffer:
            batchmessage = pickle.dumps(self.buffer)
            self.real.sendMessage(batchmessage, headers = {'buffersize':len(self.buffer)})
            del self.buffer[:]

    def disconnect(self):
        self.flush()
        self.real.disconnect()

    def __exit__(self, type, value, tb):
        self.disconnect()

    def __enter__(self):
        self.real.__enter__()
        return self

class AMQClientFactory:
    """
    A AMQClientFactory should be used to inizialize amq clients
    """

    def __init__(self, amqparams, encoder = JSONEncoder(), useDLQ = False):
        self.params = {}
        self.params['amqparams'] = {'reconnect_attempts_max': 5,
                                    'reconnect_sleep_initial': 1,
                                    'reconnect_sleep_jitter':3,
                                    'reconnect_sleep_max': 300,
                                    'try_loopback_connect': False}
        self.params['amqparams'].update(amqparams)
        self.encoder = encoder
        self.references = weakref.WeakValueDictionary()
        self.useDLQ = useDLQ

    def setMessageQueue(self, messagequeue):
        self.params['messagequeue'] = messagequeue
        qname = messagequeue.split('/').pop()
        if 'commandtopic' not in self.params:
            self.params['commandtopic'] = '/topic/%s_cmd' % qname
        if self.useDLQ and 'errortopic' not in self.params:
            self.params['errortopic'] = '/topic/%s_error' % qname

    def setCommandTopic(self, commandtopic):
        self.params['commandtopic'] = commandtopic

    def setErrorTopic(self, errortopic):
        self.params['errortopic'] = errortopic

    def createConsumerClient(self, commandtopic = None):
        if not commandtopic and not 'commandtopic' in self.params:
            raise NameError("Cannot create consumer monitor. No command queue set! Please set the commandtopic argument or call setCommandTopic before")
        cid = 'client-%s.%s.%s' % ( socket.gethostname(), 
                                    os.getpid(), 
                                    random.randrange(200))
        obj = ConsumerClient( AMQStompConnector(cid, self.params['amqparams'], self.encoder), 
                              commandtopic or self.params['commandtopic'])
        self.references[id(obj)] = obj
        return obj

    def createProducer(self, messagequeue = None, defaultheaders = None):
        if not messagequeue and not 'messagequeue' in self.params:
            raise NameError("Cannot create producer. No message queue set! Please set the messagequeue argument or call setMessageQueue before")
        cid = 'producer-%s.%s.%s' % (socket.gethostname(), 
                                     os.getpid(), 
                                     random.randrange(200))
        obj = Producer( AMQStompConnector(cid, self.params['amqparams'], self.encoder), 
                        messagequeue or self.params['messagequeue'], 
                        defaultheaders)
        self.references[id(obj)] = obj
        return obj

    def createBufferedProducer(self, buffersize, messagequeue = None, defaultheaders = None):
        if not messagequeue and not 'messagequeue' in self.params:
            raise NameError("Cannot create producer. No message queue set! Please set the messagequeue argument or call setMessageQueue before")
        cid = 'producer-%s.%s.%s' % ( socket.gethostname(), 
                                      os.getpid(), 
                                      random.randrange(200) )
        obj = BufferedProducer(
                    Producer( AMQStompConnector(cid, self.params['amqparams'], self.encoder), 
                              messagequeue or self.params['messagequeue'], 
                              defaultheaders), 
                    buffersize)
        self.references[id(obj)] = obj
        return obj

    def createErrorStrategy(self, logerrorparams = None, errordest = None, userfunction = None):
        if logerrorparams == None:
            logerrorparams = {}
        errorstrategy = ErrorStrategy()
        if logerrorparams:
            errorstrategy.append(MessageProcessingErrorEvent, ErrorLogStrategy(level=logerrorparams.get('level', logging.WARN), 
                                                                               logfullmessage=logerrorparams.get('logfullmessage', False)))
        if errordest:
            errorstrategy.add(MessageProcessingErrorEvent, ErrorDLQStrategy(errordest))
        if userfunction:
            errorstrategy.add(MessageProcessingErrorEvent, ErrorUserFunctStrategy(userfunction))
        return errorstrategy
    
    def createConsumer(self, acallable, messagequeue = None, commandtopic = None, errorstrategy = None):
        if not messagequeue and not 'messagequeue' in self.params:
            raise NameError("Cannot create consumer. No message queue set! Please set the messagequeue argument or call setMessageQueue before")

        if not commandtopic and not 'commandtopic' in self.params:
            raise NameError("Cannot create consumer. No command queue set! Please set the commandtopic argument or call setCommandTopic before")

        errorstrategy = errorstrategy or self.createErrorStrategy()

        cid = 'consumer-%s.%s.%s' % (socket.gethostname(), os.getpid(), random.randrange(200))
        obj = Consumer(AMQStompConnector(cid, self.params['amqparams'], self.encoder), 
                       AMQStompConnector(cid, self.params['amqparams'], self.encoder),
                       errorstrategy,
                       acallable, 
                       (messagequeue or self.params['messagequeue'], {'activemq.priority':0, 'activemq.prefetchSize':1}, 'client'), 
                       (commandtopic or self.params['commandtopic'], {'activemq.priority':10}, 'auto')
                      )
        self.references[id(obj)] = obj
        return obj

    def spawnConsumers(self, f, consumercount, errorstrategy = None):
        def startConsumer(c, i):
            def _startConsumer():
                #logging.config.fileConfig(os.path.join('etc', 'logging.conf'))
                global logger
                logger = logging.getLogger()
                logger.debug("Creating consumer %d" % i)
                c.connect()
                c.run()
            return _startConsumer

        processes = [ Process(target=startConsumer(self.createConsumer(f, errorstrategy=errorstrategy), i)) for i in range(consumercount)]
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
        #time.sleep(2)

    # istanzio la factory dei producer/consumer e setto i parametri
    amqfactory = AMQClientFactory(amqparams)
    try:
        amqfactory.setMessageQueue(messagequeue)
        errorstrategy = amqfactory.createErrorStrategy(logerrorparams = {'level' : logging.WARN}, errordest = '/topic/social_errors')

        # istanzio il monitor per statistiche e controllo
        monitor = amqfactory.createConsumerClient()
        monitor.connect()

        processes = amqfactory.spawnConsumers(f, 3, errorstrategy)
        # creo ed uso il producer:

        time.sleep(1)
        logger.info("PING:")
        logger.info(pf(monitor.ping()))
        # versione with(non necessita di connect e di disconnect:
        """
        with amqfactory.createProducer() as producer:
            for i in range(40):
               producer.sendMessage("messaggio%d" % i)
        """
        producer = BufferedProducer(amqfactory.createProducer(), 10)
        producer.connect()
        for i in range(15):
            producer.sendMessage("ciao")
        producer.disconnect()

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
    finally:
        amqfactory.disconnectAll()

    print "fine main"
    

if __name__ == '__main__':
    main()


