# vim:syntax=python tabstop=4 shiftwidth=4 expandtab

from xml.dom.minidom import  parse
from getopt import getopt
from events import EventDispatcher
import sys
import os
import logging
import traceback
import pickle
from threading import Lock

LevelMap = {
    4:logging.DEBUG,
    3:logging.INFO,
    2:logging.WARNING,
    1:logging.ERROR,
    'INFO':logging.INFO,
    'DEBUG':logging.DEBUG,
    'WARNING':logging.WARNING,
    'ERROR':logging.ERROR
}

def log2logging_level(level):
    return LevelMap[level] if LevelMap.has_key(level) else logging.DEBUG

DEBUG_SET = False 
LOGGER_PREFIX = 'pampas.'

class Document:
    
    def __init__ (self, data = None, debugSet = DEBUG_SET):
        self._paramMap = {}
        if data:
            self._paramMap.update(data)
        self._debugSet = debugSet
        self.logger = logging.getLogger(LOGGER_PREFIX + self.__class__.__name__)

    def Get(self, param):
        if not self._paramMap.has_key(param): 
            raise AttributeError("%s not found" % param)
        return self._paramMap[param]
    
    def Set(self, param, value):
        #self._paramMap[param] = utility.toUTF8(value)
        if self._debugSet:
            try:
                strvalue = value.encode('utf-8') if type(value) == unicode else str(value)
                if len(strvalue) > 100:
                    strvalue = strvalue[:100] + "... (omitted)"
                self.logger.debug("Setting [%s] to '<%s>': %s" % (param, str(type(value)),  strvalue))
            except Exception, ex:
                self.logger.debug("Failed printing set of %s" % param)
                self.logger.exception(ex)
        self._paramMap[param] = value
  
    def Has(self, param):
        return self._paramMap.has_key(param)
 
    def GetValue(self, param, defaultValue):
        if not self._paramMap.has_key(param):
            return defaultValue
        return self._paramMap[param]

    def ToDict(self):
        return self._paramMap.copy()
  
    def Del(self, param):
        if param in self._paramMap[param]:
            del self._paramMap[param]

    def Update(self, params):
        if self._debugSet:
            for k, v in params.items():
                self.Set(k,v)
        else:
            self._paramMap.update(params)
 
    def doc2pickle(self, pkl_outfilename):
        output = open(pkl_outfilename, 'wb')
        pickle.dump(self._paramMap, output)
        output.close()

    def pickle2doc(self, pkl_infilename):
        pkl_file = open(pkl_infilename, 'rb')
        self._paramMap = pickle.load(pkl_file)
        pkl_file.close()

    def __repr__(self):
        doc_start = "******** BEGIN DOCUMENT ********\n"
        doc_end = "********* END DOCUMENT *********"
        docStr = "".join(["**** ATTRIBUTE %s:%s\n" % (k, type(v) == unicode and v.encode("utf-8") or v) \
                                                                        for (k, v) in self._paramMap.items()])
        tmp = doc_start + docStr + doc_end
        return tmp

    def toXml(self):
        doc_start = "\t<Document>\n"
        doc_end = "\t</Document>\n"
        docStr = "".join(["\t<Field name=\"%s\"><![CDATA[%s]]></Field>\n" % \
                          (k, v) for (k, v) in self._paramMap.items()])
        return  doc_start + docStr + doc_end

"""
import mem_utils
class MemoryMonitor:
    def __init__(self):
        self.mem_status = (0,0,0)

    def update(self):
        mem_status = (mem_utils.memory(), mem_utils.resident(), mem_utils.stacksize())
        delta = (mem_status[0] - self.mem_status[0], mem_status[1] - self.mem_status[1], mem_status[2] - self.mem_status[2])
        self.mem_status = mem_status
        return delta
"""
#######################################################################################
#  Processor
#######################################################################################
class ProcessorStatus:
    OK = 1
    OK_NoChange = 2
    NotPassing = 3
    ProcessorFailure = 4
    KO = 5
    Filtered = 6

    status_map = {
        OK: 'OK',
        KO: 'KO',
        Filtered: 'Filtered',
        OK_NoChange: 'OK_NoChange',
        NotPassing: 'NotPassing',
        ProcessorFailure: 'ProcessorFailure',
        'OK': OK,
        'KO': KO,
        'Filtered':Filtered,
        'OK_NoChange': OK_NoChange,
        'NotPassing': NotPassing,
        'ProcessorFailure': ProcessorFailure
      }

    @staticmethod
    def decode(status):
        return ProcessorStatus.status_map[status] if ProcessorStatus.status_map.has_key(status) else 'UNKNOWN'

class Processor:
    def __init__ (self, name, pipeline = ''):
        self._name = name
        self._paramMap = {}
        self.logger = logging.getLogger('stages.%s.%s' % (pipeline, self.__class__.__name__))
  
    def SetLogLevel(self, level):
        self.logger.setLevel(level)

    def _setParameters(self, params):
        self._paramMap = params

    def GetParameter(self, param):
        return self._paramMap[param]

    def GetName(self):
        return self._name

    def ConfigurationChanged(self, parameters):
        raise "NotYetImplemented"

    def Process(self, uri, document):
        raise "NotYetImplemented"


class ProcessorNode:
    def __init__(self, processor):
        self.impl = processor
        self.debug_mem = False
        self._lazy = False
        self._sync = False
        self._config_lock = Lock()
        self._process_lock = Lock()
        self.logger = self.impl.logger
        self.mem_monitor = None

    def GetName(self):
        return self.impl.GetName()

    def SetLogLevel(self, level):
        self.logger.setLevel(level)
        self.impl.SetLogLevel(level)

    def IsLazy(self):
        return self._lazy

    def SetLazy(self, boolVal):
        self._lazy = boolVal

    def IsSynchronized(self):
        return self._sync

    def SetSynchronized(self, boolVal):
        self._lazy = boolVal

    def DebugMemUsage(self, bool_val):
        #if bool_val:
        #    self.mem_monitor = MemoryMonitor()
        self.debug_mem = bool_val

    def DebugMemoryEnabled(self):
        return self.debug_mem

    def ConfigurationChanged(self, parameters):
        if self.IsLazy():
            return
        with self._config_lock:
            self.impl.ConfigurationChanged(parameters)

    def Process(self, uri, document):
        """chiamato dalla pipeline!"""
        ret = None
        procname = self.impl.GetName()
        if self.DebugMemoryEnabled():
            self.mem_monitor.update()

        if self.IsLazy():
            self.logger.debug("Configuring lazy processor:%s" % procname)
            self.impl.ConfigurationChanged({})
            self._lazy = False

        if self.IsSynchronized():
            with self._process_lock:
                ret = self.impl.Process(uri, document)
        else:
            ret = self.impl.Process(uri, document)

        if self.DebugMemoryEnabled():
            #self.logger.info("***** Memory usage in byte:")
            delta = self.mem_monitor.update()
            mem_data = self.mem_monitor.mem_status
            self.logger.debug("IncrementalMem, Res, Stack: %d / %d / %s [%d / %d / %d]" % \
                              (delta[0], delta[1], delta[2], mem_data[0], mem_data[1], mem_data[2]))

        return ret

# TODO: cambiare con modulo imp
def dynamic_import(modulename, classname):
    mod = __import__(modulename)
    components = modulename.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return getattr(mod, classname)

class ProcessorXmlParser:
    @staticmethod
    def parseParams(configXml):
        params = {}
        params_xml = configXml.getElementsByTagName("param")
        for param in params_xml:
            valType = param.attributes['type'].value
            params[param.attributes['name'].value] = eval(valType)(param.attributes['value'].value)
        return params

    def ParseProcessor(processorXml, pipelineName = '', procname = None):
        load = processorXml.getElementsByTagName("load")[0]
        modulename = load.attributes['module'].value
        classname = load.attributes['class'].value
        logger = logging.getLogger('pampas.Processor')
        logger.info("[Processor] Loading processor %s in module %s" % (classname, modulename))
        clazz = dynamic_import(modulename, classname)
        #procname = processorXml.attributes['name'].value if processorXml.attributes else className
        processor = None
        if callable(clazz):
            procname = procname if procname else str(classname)
            processor = clazz(procname, pipelineName)
            processor._setParameters(ProcessorXmlParser.parseParams( processorXml.getElementsByTagName("config")[0] ))

            processorNode = ProcessorNode(processor)
            try:
                processorNode.SetLazy(load.attributes['lazy'].value.lower()[0] == "t")
            except:
                pass

            try:
                processorNode.SetSynchronized(load.attributes['synchronized'].value.lower()[0] == "t")
            except:
                pass

            log = processorXml.getElementsByTagName("logger")
            if log:
                level = log[0].attributes['level'].value
                try:
                    level = int(level)
                except:
                    pass
                processorNode.SetLogLevel(log2logging_level(level))

            #try:
            #    processorNode.DebugMemUsage(log[0].attributes['debug-mem'].value[0].upper() in ['T', 1])
            #except:
            #    pass
            
        return processorNode
    ParseProcessor = staticmethod(ParseProcessor)

# basato su una discussione interessante : http://blog.ianbicking.org/2007/09/12/re-raising-exceptions/
# modifica il messaggio dell'eccezione in place
def append_exc_str(the_exception, prefix_str):
    args = the_exception.args
    logging.error("ARG: %s" % str(the_exception.args))
    arg0 = '%s: %s' % (prefix_str, args[0]) if args else ''
    the_exception.args = (arg0,) + args[1:]

"""
class MemoryMonitor:
  def __init__(self):
    self.mem_status = (mem_utils.memory(), mem_utils.resident(), mem_utils.stacksize())

  def update(self):
    mem_status = (mem_utils.memory(), mem_utils.resident(), mem_utils.stacksize())
    delta = (mem_status[0] - self.mem_status[0], mem_status[1] - self.mem_status[1], mem_status[2] - self.mem_status[2])
    self.mem_status = mem_status
    return delta
""" 

class PipelineException(Exception):
    def __init__(self, source, msg):
        Exception.__init__(self, msg)
        self.source = source

class StageError(PipelineException):
    def __init__(self, docid, source, msg, status):
        PipelineException.__init__(self, source, msg)
        self.status = status
        self.docid = docid

#######################################################################################
#  DocumentPipeline
#######################################################################################
class DocumentPipeline(EventDispatcher):
    def __init__(self, name = ''):
        EventDispatcher.__init__(self)
        self._processors = []
        self.logger = logging.getLogger('pampas.%s' % self.__class__.__name__)
        self.name = name
        self.last_processed = (None, None)
      
    def addProcessor(self, procInstance):
        """
        @param Processor procInstance: istance to be added
        """
        self.logger.debug('Configuring stage %s' % procInstance.GetName())
        try:
            procInstance.ConfigurationChanged({})
            self._processors.append(procInstance)
            self.logger.info('Stage %s configured successfully' % procInstance.GetName())
        except:
            self.logger.critical(traceback.format_exc())

    def process(self, document, idfield = "url"):
        status = ProcessorStatus.OK

        #if document.Has("url"):
        url = document.GetValue(idfield, 'NoUrlFound')
        try:
            self.logger.info( "##### [%s] Processing %s.." % (self.name, url))
        except:
            self.logger.info( "[%s] Processing document (invalid url)" % self.name)

        self.logger.debug( "%s Stages to run.." % (str(len(self._processors))))

        for processor in self._processors:
            self.logger.info( "#####")
            self.logger.info( "##### [%s] Executing stage %s" % (self.name , processor.GetName()))

            exception_data = '' # se lo stage solleva eccezionisalvo la causa e il traceback
          # init lazy stages
            try:
                status = processor.Process(url, document)
            except Exception, exc:
                self.logger.critical('[%s.%s] Programming error: Stages should never throw exceptons!!' % (self.name, processor.GetName()))
                self.logger.error(traceback.format_exc())
                append_exc_str(exc, processor.GetName())
                status = ProcessorStatus.ProcessorFailure
                exception_data = str(exc)
                #raise 

            self.last_processed = (processor.GetName(), status)      
            self.logger.info( "[%s.%s] Stage status: %s" % (self.name, processor.GetName(), ProcessorStatus.decode(status) ))

            msg = 'OK'
            if status == ProcessorStatus.NotPassing:
                msg = 'Url %s dropped by stage: %s' % (url, processor.GetName())
                self.logger.warn( "[%s] %s" % (self.name, msg))
                self.fire(status, document, processor.GetName(), msg)
                break

            if status == ProcessorStatus.ProcessorFailure or status == ProcessorStatus.KO:
                msg = "Url %s FAILED processing stage %s!!!" % (url, processor.GetName())
                self.logger.error("[%s] %s" %  (self.name, msg))
                self.fire(status, document, processor.GetName(), msg)
                raise StageError(url, '%s.%s' % (self.name, self.last_processed[0]), 
                                 exception_data, status), None, sys.exc_info()[2]
          
            self.fire(status, document, processor.GetName(), msg)
        else: # for else
            self.logger.info( "[%s] Url %s successfully processed" % (self.name, url))

        return status

    def getProcessor(self, name):
        for proc in self._processors:
            if proc.GetName() == name:
                return proc.impl
        return None

    def getLastProcessed(self):
        return self.last_processed

    def init(self, pipelineconfigdir):
        pipeline_config = "%s/%s.xml" % (pipelineconfigdir, self.name)
        self.logger.info( "[%s] pipeline_config FILE  %s " % (self.name, pipeline_config))
        if not os.path.isfile(pipeline_config):
            self.logger.critical("Cannot find pipeline xml: %s." % pipeline_config)
        xml = parse(pipeline_config)
        processors_xml = xml.getElementsByTagName("processor")
        self.logger.info("[%s] Loading and configuring processors(%d).." % (self.name, len(processors_xml)))
        for processor_xml in processors_xml: 
            procname = processor_xml.attributes['name'].value
            processor_config = os.path.join(pipelineconfigdir, self.name, '%s.xml' % procname)
            self.logger.info('Loading config file %s' % processor_config)
            try:
                processorconfig_xml = parse(processor_config).getElementsByTagName("processor")[0]
                self.addProcessor(ProcessorXmlParser.ParseProcessor(processorconfig_xml, self.name, procname))
                self.logger.info("[%s] Processors successfully loaded" % self.name)
            except Exception, ex:
                self.logger.critical('Configuration error in stage %s.%s :%s' % \
                                     (self.name, processor_xml.attributes['name'].value, str(ex)))
                self.logger.exception(traceback.format_exc())
		raise

        self.logger.info("[%s] Loading status handlers.." % self.name)
        handlers_xml = xml.getElementsByTagName("status_handlers")
        if handlers_xml:
            for status in handlers_xml[0].getElementsByTagName("status"):
                iswellformed = reduce( lambda x,y: x and y in status.attributes, ['name', 'module', 'callback'], True)
                if not iswellformed:
                    self.logger.error("Handler for status %s is misconfigured. Check config file %s" % (status, pipeline_config))
                else:
                    self.logger.info('Loading handler for status %s' % status.attributes['name'].value)
                    event = ProcessorStatus.decode(status.attributes['name'].value)
                    modulename = status.attributes['module'].value
                    callbackname = status.attributes['callback'].value
                    module = __import__(modulename)
                    cb = getattr(getattr(module, modulename.split('.').pop()), 
                                 callbackname)
                    self.add_handler(event, cb)

        # AvE memory
        xml.unlink()
        self.logger.info("[%s] Handlers successfully loaded" % self.name)
##########################################################################
###
##########################################################################
def usage(msg=""):
    if len(msg):
        print "ERRORE: %s" % msg
    print "Usage: %s [-p <pipeline_name> -f <feed_url>" % sys.argv[0]
    print "Ex: %s -p rss -f http://nicopi.wordpress.com/feed/" % sys.argv[0]
    raise SystemExit
      
#########################################################################
# MAIN MAIN
#########################################################################
def main():
    try:
        opts, args = getopt(sys.argv[1:], "p:f:")
        opts = dict(opts)
    except:
        usage()
    pipeline = "rss"
    feed_url = None
    if opts.has_key('-p'):
        pipeline = str(opts['-p']).lower()
    if opts.has_key('-p'):
        feed_url = str(opts['-f']).lower()
    if feed_url is None:
        usage("Invalid feed_url")

    docpipeline = DocumentPipeline(pipeline)
    docpipeline.init(os.path.join(os.environ['FEEDPROC_HOME'], 'etc'))

if __name__ == '__main__':
    main()
