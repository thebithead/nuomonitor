from formatters import Formatter
import yaml

class JsonFormatter(Formatter):

    scheme = 'json'

    def __init__(self,**kwds):
        super(JsonFormatter,self).__init__(**kwds)
        
    def onValues(self,descriptions,values):
        return ("application/json", yaml.dump(values))

    def onEvent(self,event,entity_type, entity_data, event_data):
        return ("application/json", yaml.dump(dict(event=str(event)[10:],
                                                   entity_type=str(entity_type)[11:],
                                                   entity_data=entity_data,
                                                   event_data=event_data)))



    
            
