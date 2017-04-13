import sys,os

home=sys.path[0]
sys.path.insert(1,os.path.join(home,'appl'))
sys.path.insert(1,os.path.join(home,'pylib'))

import fileinput,re,pprint,time,requests,optparse,logging
import functools
from dateutil import parser
import pytz
from contextlib import closing
import cStringIO

from plugins import *
from formatters import *

counters = {}
metrics = {}
id = None

logging.basicConfig(level=logging.INFO,format='%(asctime)s:%(levelname)s %(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)

def formatTimestamp(tstamp):
    """
    Assume timezone of container is same as timezone that created the data file.
    Use --env TZ=':EST5EDT'  to set the container timezone, otherwise, defaults to UTC
    """
    dt = parser.parse(tstamp)
    timestamp = int(time.mktime(dt.timetuple()))
    return timestamp

usage = "usage: %prog [options]"
_parser = optparse.OptionParser(usage=usage)
_parser.add_option("-p", "--port", dest="port",
                  help="InfluxDB port", type="int", default=8086)
_parser.add_option("-H", "--hostname", dest="hostname",
                  help="Hostname", default="influxdb")
_parser.add_option("-F", "--full", action="store_false", dest="full", default=True)
_parser.add_option("-D", "--database", dest="db", default="nuodb")

(options, args) = _parser.parse_args()
    
influxdb = HttpPlugin(uri='http://%s:%d/write?db=%s' % (options.hostname,options.port,options.db),
                      format=InfluxFormatter())

FULL=options.full

logging.info("Timezone being used GMT%s." % time.strftime('%z'))

for line in fileinput.input(args,openhook=fileinput.hook_compressed):
    if line == '\n':
        if id is not None:
            counters[id] = metrics
            influxdb.onValues(metrics)
            id = None
    if "[" in line:
        id = line[line.find("]")+2:]
        timestamp = line[0:line.find(" [")]
        id = id[:id.find(":")+6]
        if id not in counters:
            counters[id] = {}
        elif not FULL:
            metrics = counters[id]
            for n in metrics:
                if n not in header:
                    del metrics[n]
        metrics = counters[id]
        metrics["TimeStamp"] = formatTimestamp(timestamp)
        f=line.split(" ")
        if f[-10] == "db":
            metrics["Database"] = f[-8]
        else:
            metrics["Database"] = "unknown"
    elif "=" in line:
        key,eq,value=line.split()
        metrics[key] = value

influxdb.onValues(metrics)
