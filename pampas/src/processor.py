# to be moved
from pipeline import Document, DocumentPipeline, ProcessorStatus, StageError
class PipelineProcessor:
    """
    A PipelineProcessor forwards incoming messages to a pipeline of stages
    """

    def __init__(self, pipelinename, pipelineconfigdir):
        self.pipeline = DocumentPipeline(pipelinename)
        self.pipeline.init(pipelineconfigdir)

    def makeDocument(self, header, message):
        if isinstance(message, dict):
            doc = Document(message)
            dpc.Set('amqheaders', header)
        elif isinstance(message, str):
            doc = Document(header)
            doc.Set('body', message)
        else:
            raise Exception("Not implemented")
        return doc

    def process(self, header, message):
        doc = self.makeDocument(header, message)
        try:
            status = self.pipeline.process(doc)
            if status not in [ProcessorStatus.OK, ProcessorStatus.OK_NoChange]:
                logger.error("Error processing %s in %s" % (header['message-id'], self.pipeline.getLastProcessed()))
        except StageError, ex:
            logger.warning("Unhandled stage error: %s" % str(ex))
            logger.exception(ex)

    def __call__(self, header, message):
        self.process(header, message)

