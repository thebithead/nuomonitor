from format import Formatter
import yaml,sys
import cStringIO,time
from contextlib import closing

def count(k,values):
    raw = values[k]
    return "raw=%si" % (raw)

def number(k,values):
    raw = values[k]
    return "raw=%si" % (raw)

def unknown(k,values):
    raw = values[k]
    return "unknown=%si" % (raw)

def percent(k,values):
    raw = values[k]
    rvalues = "raw=%si" % (raw)
    if 'NumberCores' in values:
        ncores = int(values['NumberCores'])
        if ncores != 0:
            value = int(raw)*1./ncores
            bycore= int(raw)*.01
            # norm   -> 0 <-> 100 * ncores
            # ncores -> 0 <-> ncores
            rvalues += ",norm=%f,ncores=%f" % (value,bycore)
            if k == 'PercentCpuTime':
                idle = 100*ncores - int(raw)
                # idle   -> 0 <-> 100 * ncores
                # nidle  -> 0 <-> ncores
                rvalues += ",idle=%di,nidle=%f" % (idle,float(idle)/ncores)
        else:
            # ncores only 0 at disconnect
            rvalues += ",norm=%f,ncores=%f" % (0.,0.)
            if k == 'PercentCpuTime':
                rvalues += ",idle=%di,nidle=%f" % (0.,0.)
    return rvalues

def delta(k,values):
    raw = int(values[k])
    rvalues="raw=%di" % (raw)
    if 'Milliseconds' in values:
        ms = int(values['Milliseconds'])
        rate = raw*1000./ms if ms != 0 else 0.
        rvalues += ",rate=%f" % (rate)
    return rvalues

def milliseconds(k,values):
    raw = int(values[k])
    rvalues="raw=%di" % (raw)    
    if 'Milliseconds' in values:
        ms = int(values['Milliseconds'])
        value =  raw*1./ms if ms != 0 else 0.
        rvalues+= ",value=%f" % (value)
        if 'NumberCores' in values:
            ncores = int(values['NumberCores'])
            nvalue = value/ncores if ncores != 0 else 0.
            rvalues += ",normvalue=%f" % (nvalue)
                
    return rvalues

# listed because some mappings are different than what descriptions tells us
# should handle exceptions only and map defaults.
mapper = {
    'ArchiveQueue': count,
    'DiskWritten': delta,
    'JournalQueue': count,
    'JrnlBytes': delta,
    'JrnlWrites': delta,
    'LogMsgs': count,
    'Objects': count,
    'PageFaults': count,
    'PurgedObjects': count,
    'Commits': delta,
    'Rollbacks': delta,
    'Deletes': delta,
    'Inserts': delta,
    'Updates': delta,
    'ObjectsBounced': delta,
    'ObjectsCreated': delta,
    'ObjectsDeleted': delta,
    'ObjectsDropped': delta,
    'ObjectsDroppedPurged': delta,
    'ObjectsExported': delta,
    'ObjectsImported': delta,
    'ObjectsLoaded': delta,
    'ObjectsPurged': delta,
    'ObjectsReloaded': delta,
    'ObjectsRequested': delta,
    'ObjectsSaved': delta,
    'AdminReceived': delta,
    'AdminSent': delta,
    'ClientReceived': delta,
    'ClientSent': delta,
    'MessagesReceived': delta,
    'MessagesSent': delta,
    'PacketsReceived': delta,
    'PacketsSent': delta,
    'ServerReceived': delta,
    'ServerSent': delta,
    'BytesBuffered': delta,
    'BytesReceived': delta,
    'BytesSent': delta,
    'IdManagerBlockingStallCount': delta,
    'IdManagerNonBlockingStallCount': delta,
    'PendingUpdateStallCount': delta,
    'PlatformCatalogStallCount': delta,
    'Stalls': delta,
    'ArchiveBufferedBytes': delta,
    'CheckCompleteFull': delta,
    'CheckCompleteOptimized': delta,
    'NumSplits': delta,
    'SnapshotAlbumsClosed': delta,
    'SnapshotAlbumsClosedForGC': delta,
    'SqlMsgs': delta,
    'ActiveTime': milliseconds,
    'IdleTime': milliseconds,
    'ArchiveBandwidthThrottleTime': milliseconds,
    'ArchiveDirectoryTime': milliseconds,
    'ArchiveFsyncTime': milliseconds,
    'ArchiveReadTime': milliseconds,
    'ArchiveSyncThrottleTime': milliseconds,
    'ArchiveWriteTime': milliseconds,
    'JournalBandwidthThrottleTime': milliseconds,
    'JournalDirectoryTime': milliseconds,
    'JournalFsyncTime': milliseconds,
    'JournalWriteTime': milliseconds,
    'Milliseconds': milliseconds,
    'UserMilliseconds': milliseconds,
    'KernelMilliseconds': milliseconds,
    'SqlListenerIdleTime': milliseconds,
    'SqlListenerSqlProcTime': milliseconds,
    'SqlListenerStallTime': milliseconds,
    'SqlListenerThrottleTime': milliseconds,
    'PendingEventsCommitTime': milliseconds,
    'PendingInsertWaitTime': milliseconds,
    'PendingUpdateWaitTime': milliseconds,
    'PlatformObjectCheckCompleteTime': milliseconds,
    'PlatformObjectCheckOpenTime': milliseconds,
    'PlatformObjectCheckPopulatedTime': milliseconds,
    'TransactionBlockedTime': milliseconds,
    'AtomProcessingThreadBacklog': milliseconds,
    'BroadcastTime': milliseconds,
    'ClientThreadBacklog': milliseconds,
    'CreatePlatformRecordsTime': milliseconds,
    'HTTPProcessingThreadBacklog': milliseconds,
    'LoadObjectTime': milliseconds,
    'LocalCommitOrderTime': milliseconds,
    'MemoryThrottleTime': milliseconds,
    'MessageSequencerMergeTime': milliseconds,
    'MessageSequencerSortTime': milliseconds,
    'NodeApplyPingTime': milliseconds,
    'NodePingTime': milliseconds,
    'NodePostMethodTime': milliseconds,
    'NodeSocketBufferWriteTime': milliseconds,
    'NonChairSplitTime': milliseconds,
    'PruneAtomsThrottleTime': milliseconds,
    'RefactorTXQueueTime': milliseconds,
    'RemoteCommitTime': milliseconds,
    'StallPointWaitTime': milliseconds,
    'SyncPointWaitTime': milliseconds,
    'WaitForSplitTime': milliseconds,
    'WriteThrottleTime': milliseconds,
    'CurrentActiveTransactions': number,
    'CurrentCommittedTransactions': number,
    'CurrentPurgedTransactions': number,
    'OldestActiveTransaction': number,
    'OldestCommittedTransaction': number,
    'HeapActive': number,
    'HeapAllocated': number,
    'HeapMapped': number,
    'Memory': number,
    'ClientCncts': number,
    'DependentCommitWaits': number,
    'NumberCores': number,
    'ObjectFootprint': number,
    'SendQueueSize': number,
    'SendSortingQueueSize': number,
    'SocketBufferBytes': number,
    'SnapshotAlbumSize': number,
    'SnapshotAlbumTime': number,
    'WriteLoadLevel': number,
    'WriteThrottleSetting': number,
    'PercentCpuTime': percent,
    'PercentSystemTime': percent,
    'PercentUserTime': percent
}

def summary(values):
    # from nuodbmgr
    def get(key):
        return int(values[key]) if key in values else 0

    activeTime = get("ActiveTime")
    deltaTime = get("Milliseconds")
    idleTime  = get("IdleTime")

    def v(raw):
        rvalues = "raw=%d" % (raw)
        multiplier = (deltaTime-idleTime)*1./deltaTime
        if activeTime > 0:
            rvalues += ",percent=%d" % (int(round(multiplier*raw*100./activeTime)))
        if deltaTime > 0:
            rvalues += ",nthreads=%d" % (int(round(raw/deltaTime)))
        return rvalues

    cpuTime   = get("UserMilliseconds") + get("KernelMilliseconds")
    syncTime  = get("SyncPointWaitTime") + get("StallPointWaitTime")
    syncTime -= get("PlatformObjectCheckOpenTime") + get("PlatformObjectCheckPopulatedTime") + get("PlatformObjectCheckCompleteTime")
    lockTime  = get("TransactionBlockedTime")
    fetchTime = get("PlatformObjectCheckOpenTime") + get("PlatformObjectCheckPopulatedTime") + get("PlatformObjectCheckCompleteTime") + get("LoadObjectTime")
    commitTime       = get("RemoteCommitTime")
    ntwkSendTime     = get("NodeSocketBufferWriteTime")
    archiveReadTime  = get("ArchiveReadTime")
    archiveWriteTime = get("ArchiveWriteTime") + get("ArchiveFsyncTime") + get("ArchiveDirectoryTime")
    journalWriteTime = get("JournalWriteTime") + get("JournalFsyncTime") + get("JournalDirectoryTime")
    throttleTime     = get("ArchiveSyncThrottleTime") + get("MemoryThrottleTime") + get("WriteThrottleTime")
    throttleTime    += get("ArchiveBandwidthThrottleTime") + get("JournalBandwidthThrottleTime")
    values = { 
        "Summary.Active" : v(activeTime),
        "Summary.CPU" : v(cpuTime),
        "Summary.Idle" : v(idleTime),
        "Summary.Sync" : v(syncTime),
        "Summary.Lock" : v(lockTime),
        "Summary.Fetch": v(fetchTime),
        "Summary.Commit": v(commitTime),
        "Summary.NtwkSend":v(ntwkSendTime),
        "Summary.ArchiveRead":v(archiveReadTime),
        "Summary.ArchiveWrite":v(archiveWriteTime),
        "Summary.JournalWrite":v(journalWriteTime),
        "Summary.Throttle":v(throttleTime)
    }
    return values


class InfluxFormatter(Formatter):

    scheme = "influxdb"

    def __init__(self,**kwds):
        super(InfluxFormatter,self).__init__(**kwds)
        
    header = ["TimeStamp","NodeType","Hostname","ProcessId","NodeId","Database"]

    def onValues(self,descriptions,values):
        global mapper
        # metric,<identity tags> <fields> timestamp
        timestamp = values['TimeStamp']
        timestamp = int(timestamp*1000000000)
        nodetype  = values['NodeType']
        hostname  = values['Hostname']
        processid = values['ProcessId']
        nodeid    = values['NodeId']
        database  = values['Database']

        tags = "host=%s,nodetype=%s,pid=%s,nodeid=%s,db=%s" % (hostname,nodetype,processid,nodeid,database)

        with closing(cStringIO.StringIO()) as buffer:
            for k in values:
                rvalues = mapper[k](k,values) if k in mapper else unknown(k,values)
                print >> buffer,"%s,%s %s %s" % (k,tags,rvalues,timestamp)
            summary_map = summary(values)
            for key,rvalues in summary_map.iteritems():
                print >> buffer,"%s,%s %s %s" % (key,tags,rvalues,timestamp)
            toinflux = buffer.getvalue()

        return ("text/plain; charset=us-ascii", toinflux)

    def onEvent(self,event,entity_type, entity_data, event_data):
        with closing(cStringIO.StringIO()) as buffer:
            print >> buffer,"%s,event=%s" % (str(entity_type)[11:],str(event)[10:]),
            for k,v in entity_data.iteritems():
                print >> buffer,"%s=%s" % (k,v),
            toinflux = buffer.getvalue()
            toinflux = toinflux.replace(' ',',') + " value=%d %d" % (int(event),int(time.time()*1000000000))
        return ("text/plain; charset=us-ascii", toinflux)        

if __name__ == '__main__':
    import yaml,sys
    descriptions,values = [ data for data in yaml.load_all(sys.stdin) ]
    class Identity(object):
        def __init__(self,**kwds):
            self.__dict__ = kwds
        def toValues(self):
            return dict(Database=self.database,
                        NodeId=self.nodeid,
                        Hostname=self.hostname,
                        ProcessId=self.processid,
                        NodeType=self.nodetype)
    identity = Identity(database  = values['Database'],
                        nodeid    = values['NodeId'],
                        hostname  = values['Hostname'],
                        processid = values['ProcessId'],
                        nodetype  = values['NodeType'])
    
    influxf = InfluxFormatter()
    (ct,d) = influxf(identity,descriptions,values)
    print ct
    print d

