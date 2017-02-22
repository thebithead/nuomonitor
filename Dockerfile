FROM python:2-alpine
MAINTAINER Dirk Butson "dbutson@nuodb.com"

#added python dependencies in pylib - can't add stompest there
RUN pip install stompest
#    && \
#    pip install pynuodb  && \
#    pip install pypubsub && \
#    pip install requests && \
#    pip install tornado  && \
#    pip install wrapt    && \
#    pip install pyaml

ADD . /home/metrics
EXPOSE 80
WORKDIR /home/metrics
ENTRYPOINT ["./entrypoint.sh"]
