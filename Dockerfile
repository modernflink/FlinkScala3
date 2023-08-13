FROM flink:1.16.2-scala_2.12-java11
RUN rm $FLINK_HOME/opt/flink-cep-scala_2.12-1.16.2.jar
RUN rm $FLINK_HOME/lib/flink-scala_2.12-1.16.2.jar
RUN mkdir -p $FLINK_HOME/usrlib
COPY --chown=flink:flink target/scala-3.3.0/my-flink-scala-proj-assembly-0.1.0-SNAPSHOT.jar /opt/flink/usrlib/my-flink-job.jar
