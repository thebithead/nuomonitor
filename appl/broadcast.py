from monitor import MetricsListener,EventListener
from util import *
from pubsub  import pub
import threading

__all__=['MetricsProducer', 'EventProducer']


class EventProducer(EventListener):
    def __init__(self):
        super(EventListener,self).__init__()
        self.cache = { EventListener.EntityType.PEER     : [],
                       EventListener.EntityType.PROCESS  : [],
                       EventListener.EntityType.DATABASE : []
        }
        pub.subscribe(self.onEventRequest,'event.sync.request')

    def onEvent(self, event, entity_type, entity_data, event_data):
        if entity_type in self.cache:
            if event == EventListener.EventType.JOINED:
                cached = self.cache[entity_type]
                cached.append(entity_data)
            elif event == EventListener.EventType.LEFT:
                cached = self.cache[entity_type]
                for obj,ndx in enumerate(cached):
                    if obj == entity_data:
                        del obj[ndx]
                        break
        pub.sendMessage('event.queue',event=event, entity_type=entity_type, entity_data=entity_data, event_data=event_data)
        pass

    def onEventRequest(self,replyTo):
        peers = self.cache[EventListener.EntityType.PEER]
        for edata in peers:
            pub.sendMessage(replyTo, event=EventListener.EventType.JOINED, entity_type=EventListener.EntityType.PEER,
                            entity_data=edata, event_data=None)
        dbs = self.cache[EventListener.EntityType.DATABASE]
        for edata in dbs:
            pub.sendMessage(replyTo, event=EventListener.EventType.JOINED, entity_type=EventListener.EntityType.DATABASE,
                            entity_data=edata, event_data=None)
        processes = self.cache[EventListener.EntityType.PROCESS]
        for edata in processes:
            pub.sendMessage(replyTo, event=EventListener.EventType.JOINED, entity_type=EventListener.EntityType.PROCESS,
                            entity_data=edata, event_data=None)

class MetricsProducer(MetricsListener):
    def __init__(self):
        super(MetricsProducer,self).__init__()
        self.identity = None

    @print_exc
    def onStart(self,metrics):
        super(MetricsProducer,self).onStart(metrics)
        pub.sendMessage('metrics.description',description=self.metrics)
        pub.subscribe(self.onMetricsRequest,'metrics.request.description')
        pub.subscribe(self.onValuesRequest,'metrics.request.values')

    @print_exc        
    def onChange(self,values):
        if len(self.values) > 0:
            pub.sendMessage('metrics.values.updated',identity=self.getIdentity(),body=values)
        super(MetricsProducer,self).onChange(values)
        pub.sendMessage('metrics.values.full',identity=self.getIdentity(),body=self.values)

    @print_exc        
    @trace
    def onMetricsRequest(self,replyTo):
        pub.sendMessage(replyTo,description=self.metrics)

    @print_exc        
    @trace
    def onValuesRequest(self,replyTo):
        pub.sendMessage(replyTo,identity=self.identity,body=self.values)

    def getIdentity(self):
        class Identity(object):
            def __init__(self,**kwds):
                self.__dict__ = kwds
            def toValues(self):
                return dict(Database=self.database,
                            NodeId=self.nodeid,
                            Hostname=self.hostname,
                            ProcessId=self.processid,
                            NodeType=self.nodetype)
            
        if self.identity is None:
            values = self.values
            self.identity = Identity(database  = values['Database'],
                                     nodeid    = values['NodeId'],
                                     hostname  = values['Hostname'],
                                     processid = values['ProcessId'],
                                     nodetype  = values['NodeType'])
        return self.identity
            

