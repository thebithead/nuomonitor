from .plugin import Plugin
from util import print_exc
import requests

__all__ = ['HttpPlugin']

class HttpPlugin(Plugin):
    """ post http..."""

    scheme = 'http'
    
    @print_exc
    def __init__(self,**kwds):
        super(HttpPlugin,self).__init__(**kwds)

    @print_exc
    def onContent(self,message,contentType):
        arg = requests.post(self.uri,
                            headers={"Content-Type" : contentType},
                            data=message)

