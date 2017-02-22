This directory contains the client and server for nuomonitor.

You can build a docker container of nuomonitor with

docker build -t nuomonitor:latest .

If you wish to install the grafana/influxdb stack and publish metrics to that you'll need to:

1) run ./bin/manage.sh start
2) create and run the nuomonitor container
   % docker create -p 8000:80 --name nuomonitor nuomonitor:latest -b <broker> -p <password>
   % docker start nuomonitor

The -p 8000:80 exposes port 8000 as the port to use when communicating to nuomonitor.

You can verify that nuomonitor is running okay by:

- get latest data (if you have engines running in domain you pointed to)
  % bin/e get latest

- look at docker logs
  % docker logs nuomonitor

3) start a monitor publisher to publish to your influxdb
   - this consist of the bin/e script and the file test/influxdb.test
   - if you are not running the whole stack on your local machine you might want to modify
     bin/e so that you update test/influxdb.test to point to the correct influxdb server.
     you might also modify bin/e if your nuomonitor instance is not running on the local host.
   % bin/e post test/influxdb.test

Known issues:

A. I have had problems where I was unable to connect to an engine.  If
   this occurs the start process can hang on you.  If you don't see
   the following output line in the log, you are most likely hung up.

   All engines connected to ...



