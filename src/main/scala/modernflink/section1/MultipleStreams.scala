package modernflink.section1

import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.{ConnectedStreams, DataStream, KeyedStream, StreamExecutionEnvironment}
import modernflink.model.{HumidityReading, LocalSummary, TemperatureReading}

val env = StreamExecutionEnvironment.getExecutionEnvironment
