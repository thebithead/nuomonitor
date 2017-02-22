#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os, sys, logging, signal, time
import json, urllib, urlparse

home=sys.path[0]
sys.path.insert(1,os.path.join(home,'appl'))
sys.path.insert(1,os.path.join(home,'pylib'))

import tornado.web, tornado.template, tornado.ioloop, tornado.httpserver, requests, traceback
from util import *
from plugins import *
from formatters import *

#logging.basicConfig(level=logging.INFO)

class RunHandler(tornado.web.RequestHandler):
    """
    # POST   /api/v1/metrics                  => creates a new instance
    # DELETE /api/v1/metrics/<instanceid>     => stops a running instance
    # GET    /api/v1/metrics                  => returns a list of running instances
    # GET    /api/v1/metrics/description      => returns description of each metric that could be returned
    # GET    /api/v1/metrics/latest           => pull current values (input document is filter definiton)
    # GET    /api/v1/metrics/<instanceid>     => status of running instance
    """

    SUPPORTED_METHODS = ['POST','GET', 'DELETE']
    plugin_instances = {}

    @trace
    def initialize(self, **kwds):
        self.env = kwds

    @trace
    def post(self, instanceid = None):
        """ Starts a notifier """

        # instanceid should not be used on a post.
        if instanceid is not None:
            self.set_status(403)
            self.finish()
            return
        
        try:
            data = json.loads(self.request.body) if self.request.body else None
        except:
            data = json.loads(urllib.unquote_plus(self.request.body))

        (ecode, result) = self.start_plugin(data)

        if ecode >= 200 and ecode < 300:
            self.add_header('Location', result['link'])
        
        self.write(json.dumps( result ))
        self.set_status(ecode)
        self.finish()

    @trace
    def delete(self, instanceid):
        """ stop instance if exists """
        if instanceid in self.plugin_instances:
            ecode,result   = self.stop_plugin(instanceid)
            self.write(json.dumps(result))
            self.set_status(ecode)
        else:
            self.write("plugin instance %s not found." % (instanceid))
            self.set_status(404)
        self.finish()
        

    @trace
    def get(self, instanceid = None):
        """
        # GET    /api/v1/metrics                  => returns a list of running instances
        # GET    /api/v1/metrics/<instanceid>     => status of running instance
        """
        results = []

        # get candidates

        pull = False
        singleton = False
        print 'instanceid:',instanceid
        if instanceid is None:
            instances = self.plugin_instances.keys()
        elif instanceid in self.plugin_instances:
            singleton = True
            instances = [ instanceid ]
        else:
            if instanceid in ['latest','description']:
                pull = True
            else:
                self.set_status(404)
                self.finish()
                return

        if not pull:
            print 'get instances:',instances
            for instanceid in instances:
                result = self.get_plugininfo(instanceid)
                results.append(result)
            if len(results) == 0:
                self.set_status(204)
            else:
                self.add_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.add_header("Pragma", "no-cache")
                self.add_header("Expires", "0")
                if singleton:
                    self.write(json.dumps(results[0]))
                else:
                    self.write(json.dumps(sorted(results,key=lambda plugin: plugin['link'])))
                self.set_status(200)
        else:
            try:
                data = json.loads(self.request.body) if self.request.body else None
            except:
                data = json.loads(urllib.unquote_plus(self.request.body))
            class Retrieve(Plugin):
                def __init__(self,**kwds):
                    super(Retrieve,self).__init__(**kwds)
                    # self.descriptions exist in base
                    self.values = []
                    self.request_metrics()
                def onValues(self,values):
                    self.values.append(values)

            results = Retrieve()
            if instanceid == 'description':
                self.write(json.dumps(results.descriptions))
            elif instanceid == 'latest':
                self.write(json.dumps(results.values))
            else:
                self.write("Unknown operation.")
                self.set_status(500)
            del results
            
        self.finish()

    @trace
    def get_plugininfo(self,instanceId,appendId=False):
        instance = self.plugin_instances[instanceId]
        url = 'http://%s%s' % (self.request.host,self.request.uri)
        if appendId:
            url = '%s/%s' % (url,str(instanceId))
        return { 'instanceId' : instanceId,
                 'config'     : instance.to_dict(),
                 'link'       : url
        }

    PLUGINS    = Plugin.plugins()
    FORMATTERS = Formatter.formatters()
    
    @trace
    def start_plugin(self,data):
        # data has some required fields.  Other fields can be
        # plugin specific...
        """
        {
           'uri' : <identifier>://....,
           'filter' : {
               'database'  : ...
               'host'      : ...
               'processid' : ...
           },
           'dataset' : 'changes|full',
           'format'  : 'json'
        }
        """
        try:
            if 'uri' in data:
                uri = data['uri']
                parts = urlparse.urlparse(uri)
                # get the plugin
                if parts.scheme in self.__class__.PLUGINS:
                    plugin = self.__class__.PLUGINS[parts.scheme]
                else:
                    return (500,"Don't know how to publish to %s" % (uri))
            else:
                return (500,"must specify uri in json.")
            

            # create filter -- currently we filter by database, nodie, host, processid
            # if any of these are specified then only data from a process that matches that criteria is 
            # sent to the plugin.
            if 'filter' in data:
                data['filter'] = self.create_filter(**data['filter'])
            else:
                data['filter'] = self.create_filter()
            if 'format' in data:
                scheme = data['format']
                if scheme in self.__class__.FORMATTERS:
                    data['format'] = self.__class__.FORMATTERS[scheme](**data)
                else:
                    return (500,'scheme %s not in FORMATTERS' % (scheme))
            else:
                data['format'] = self.__class__.FORMATTERS['json'](**data)

            print 'start plugin: ',plugin
            obj = plugin(**data)
            obj.start()
            self.plugin_instances[obj.instanceId] = obj
            pluginfo = self.get_plugininfo(obj.instanceId,appendId=True)
            return (201, pluginfo)
        except:
            traceback.print_exc()
            pass
        return (500,None)

    def create_filter(self,**data):
        class filter:
            def __init__(self,**kwds):
                for k,v in kwds.iteritems():
                    setattr(self,k,v)
                self.host     = getattr(self,'host',None)
                self.pid      = getattr(self,'pid',None)
                self.database = getattr(self,'database',None)
                self.nodeid   = getattr(self,'nodeid',None)
                self.nodetype = getattr(self,'nodetype',None)
            def __call__(self,identity):
                if self.host and self.host != identity.hostname:
                    return False
                if self.pid and self.pid != identity.processid:
                    return False
                if self.database and self.database != identity.database:
                    return False
                if self.nodeid and self.nodeid != identity.nodeid:
                    return False
                if self.nodetype and self.nodetype != identity.nodetype:
                    return False
                return True
            def to_dict(self):
                return dict([ (k,v) for k,v in self.__dict__.iteritems() if v is not None ])
        return filter(**data)


    @trace
    def stop_plugin(self,instanceId):
        #
        #
        if instanceId in self.plugin_instances:
            instance = self.plugin_instances[instanceId]
            data = self.get_plugininfo(instanceId)
            del data['link']
            instance.stop()
            del self.plugin_instances[instanceId]
            return (300,data)
        else:
            return (400,None)


def stop_server(signum, frame):
    tornado.ioloop.IOLoop.instance().stop()
    logging.info('Stopped!')

def run(port,broker,password,user=None):

    application = tornado.web.Application([
        (r'/api/v1/metrics$',          RunHandler),
        (r'/api/v1/metrics/([^/]+)?$', RunHandler),
        ])
    commands = """
    # POST   /api/v1/metrics                  => creates a new instance
    # DELETE /api/v1/metrics/<instanceid>     => stops a running instance
    # GET    /api/v1/metrics                  => returns a list of running instances
    # GET    /api/v1/metrics/description      => return description
    # GET    /api/v1/metrics/latest           => return latest values
    # GET    /api/v1/metrics/<instanceid>     => status of running instance
    """

    signal.signal(signal.SIGINT, stop_server)

    print 'Serving HTTP on 0.0.0.0 port %d ...' % (port)
    print commands
    print 'Connecting to all engines from %s' % (broker)
    sys.stdout.flush()
    Plugin.start_source(broker=broker,password=password,user=user)
    print 'All engines connected to ...'
    sys.stdout.flush()
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(port)
    try:
        tornado.ioloop.IOLoop.instance().start()
    except:
        pass
    finally:
        Plugin.stop_source()

if __name__ == '__main__':
    import optparse
    
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-b", "--broker", dest="broker", help="Broker hostname or IP address",
                      default="127.0.0.1")
    parser.add_option("-u", "--user",     dest="user", help="Domain user name", default="domain")
    parser.add_option("-p", "--password", dest="password", help="Domain user password", default="bird")
    parser.add_option("-l", "--listen",   dest="listen", help="Port to listen", type=int, default=80)
    (options, args) = parser.parse_args()
    run(options.listen,options.broker,options.password,options.user)
