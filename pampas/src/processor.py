# vim:syntax=python tabstop=4 shiftwidth=4 expandtab
# to be moved
from pipeline import Document, DocumentPipeline, ProcessorStatus, StageError
import json
import logging
from core import StopConsumerError

logger = logging.getLogger('pampas.processor')

class PipelineProcessor(object):
    """
    A PipelineProcessor forwards incoming messages to a pipeline of stages
    """

    def __init__(self, pipelinename, pipelineconfigdir, debugDocumentSet = False, idfield = "url", defaultparams = None, maxdocs = 0):
        self.pipeline = DocumentPipeline(pipelinename)
        self.pipeline.init(pipelineconfigdir)
        self.idfield = idfield
        self.debugDocumentSet = debugDocumentSet
        self.maxdocs = maxdocs
        self.processed = 0
        self.defaultparams = defaultparams or {}

    def makeDocument(self, header, message):
        if isinstance(message, dict):
            doc = Document(message, self.debugDocumentSet)
            doc.Set('amqheaders', header)
        elif isinstance(message, str):
            doc = Document(header, self.debugDocumentSet)
            doc.Set('body', message)
        else:
            raise Exception("Not implemented")

        for key, val in self.defaultparams.items():
            doc.Set(key, val)
        return doc

    def process(self, header, message):
        doc = self.makeDocument(header, message)
        try:
            status = self.pipeline.process(doc, idfield = self.idfield)
            if status not in [ProcessorStatus.OK, ProcessorStatus.OK_NoChange]:
                logger.error("Error processing %s in %s" % (header['message-id'], self.pipeline.getLastProcessed()))
        except StageError, ex:
            logger.warning("Unhandled stage error: %s" % str(ex))
            logger.exception(ex)

        self.processed += 1
        if self.maxdocs > 0 and self.processed >= self.maxdocs:
            raise StopConsumerError()

    def __call__(self, header, message):
        self.process(header, message)


