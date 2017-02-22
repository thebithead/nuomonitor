from .plugin         import Plugin
from stompest.config import StompConfig
from stompest.sync   import Stomp
import urlparse

__all__=['StompPlugin']

class StompPlugin(Plugin):
    """
    output formats:
        . json
        . graphite
        . influxdb
    """
    scheme = "stomp"
    
    def __init__(self,**kwds):
        super(StompPlugin,self).__init__(**kwds)
        url = self.uri
        parts = urlparse.urlparse(url)
        host = parts.netloc
        if ':' not in host:
            host = host + ":61613"
        self.client = Stomp(StompConfig("tcp://%s" % (host)))
        self.client.connect()
        self.QUEUE  = parts.path

    def stop(self):
        super(StompPlugin,self).stop()
        self.client.disconnect()

    def onContent(self,message,contentType):
        self.client.send(self.QUEUE, message)
