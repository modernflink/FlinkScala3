ARG FLINK_VERSION
FROM flink:${FLINK_VERSION}-scala_2.12-java11

# https://docs.docker.com/engine/reference/builder/#understand-how-arg-and-from-interact
ARG FLINK_VERSION

# Grab coursier to download jars from maven
RUN curl -fL "https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-linux.gz" | gzip -d > cs
RUN chmod +x cs
RUN mv `./cs fetch org.flinkextended::flink-scala-api:${FLINK_VERSION}_1.1.0` /opt/flink/lib
RUN rm cs
RUN rm $FLINK_HOME/opt/flink-cep-scala_2.12-*.jar
RUN rm $FLINK_HOME/lib/flink-scala_2.12-*.jar
RUN mkdir -p $FLINK_HOME/usrlib
COPY --chown=flink:flink target/scala-3.3.0/my-flink-scala-proj-assembly-0.1.0-SNAPSHOT.jar /opt/flink/usrlib/my-flink-job.jar
