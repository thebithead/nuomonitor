from .plugin import Plugin
from monitor import print_exc
import socket,urlparse

__all__ = ['TcpPlugin']
                            
class TcpPlugin(Plugin):
    """ put tcp..."""

    scheme = 'tcp'

    @print_exc
    def __init__(self, **kwds):
        super(TcpPlugin,self).__init__(**kwds)
        self.sock = None
        pass

    @print_exc
    def __del__(self):
        if self.sock is not None:
            self.sock.close()
        super(TcpPlugin,self).__del__()
        
    @print_exc
    def onContent(self,message,contentType):
        try:
            if self.sock is None:
                # Create a TCP/IP socket
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # Connect the socket to the port where the server is listening
                url = self.uri
                parts = urlparse.urlparse(url)
                host,port = parts.netloc.split(':')
                self.server_address = (host,int(port))
                self.sock.connect(self.server_address)
            if self.sock is not None:
                self.sock.sendall(message)
        except socket.error, msg:
            print "A socket error occurred:", msg
            if self.sock is not None:
                self.sock.close()
                self.sock = None
            
