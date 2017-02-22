from broadcast import *
import yaml
from monitor import get_nuodb_metrics
from util import *

"""
output formats:
. json
. graphite
. influxdb
"""
__all__=['Plugin']

class Plugin(MetricsConsumer,EventConsumer):

    source = None

    @classmethod
    def start_source(cls,broker,password,user='domain'):
        cls.source = get_nuodb_metrics(broker=broker,
                                       password=password,
                                       user=user,
                                       listener=MetricsProducer,
                                       domain_listener=EventProducer)

    @classmethod
    def stop_source(cls):
        cls.source.disconnect()

    @staticmethod
    def plugins():
        return dict([ (p.scheme, p) for p in Plugin.__subclasses__() ])
            
    def __init__(self,**kwds):
        EventConsumer.__init__(self,**kwds)
        MetricsConsumer.__init__(self,**kwds)
        self.descriptions = {}
        self.formatter = getattr(self,'format',self.defaultFormatter)

    def onContent(self, content, contentType):
        """  str content     -- data encoded in string format
             str contentType -- based upon formatter,
        """
        pass
    
    def start(self):
        EventConsumer.start(self)
        MetricsConsumer.start(self)
        
    def stop(self):
        EventConsumer.stop(self)
        MetricsConsumer.stop(self)
        del self

    def to_dict(self):
        return { 'uri'    : self.uri,
                 'mode'   : self.mode,
                 'format' : str(getattr(self,'format','json')),
                 'filter' : self.filter.to_dict()
               }

    @print_exc
    def onMetrics(self,description):
        self.descriptions = description

    @print_exc
    def onValues(self,values):
        contentType,msg = self.formatter.onValues(self.descriptions,values)
        self.onContent(msg,contentType)

    @print_exc
    def onEvent(self,event, entity_type, entity_data, event_data):
        contentType,msg = self.formatter.onEvent(event,entity_type, entity_data, event_data)
        self.onContent(msg,contentType)
        
    @classmethod
    def defaultFormatter(cls,descriptions,values):
        """ serialize data into json representation """
        return ("application/json",yaml.dump(values))

