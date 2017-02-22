
class Formatter(object):

    def __init__(self,**kwds):
        for k,v in kwds.iteritems():
            setattr(self,k,v)

    def __str__(self):
        return self.__class__.scheme

    def onEvent(self,event,entity_type, entity_data, event_data):
        logging.warn("formatter [%s] does not override onEvent." % (self.scheme))
    
    def onValues(self,descriptions,values):
        logging.warn("formatter [%s] does not override onValues." % (self.scheme))

    @staticmethod
    def formatters():
        return dict([ (f.scheme, f) for f in Formatter.__subclasses__() ])

    
