
from functools import wraps, partial

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

    def _find_handlers(self, obj):
       return filter(lambda x: callable(x) and hasattr(x, 'events'),
                map(partial(getattr, obj), dir(obj)))

    def attach_listener(self, listener):
        # iterating over class method to find and register annotated handlers
        for method in self._find_handlers(listener):
            for event_type in method.events:
                self.add_handler(event_type, method)
    
    def detach_listener(self, listener):
        for method in self._find_handlers(listener):
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
    The handle_events decorator indicates that the decorated method can handle the specified event types.
    Call dispatcher.attach_listener(methodOwner) to bind the listener to an event source.
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


