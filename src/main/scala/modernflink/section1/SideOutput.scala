package modernflink.section1

import modernflink.model.HumidityReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.{OutputTag, StreamExecutionEnvironment}

class FireAlert() extends ProcessFunction[HumidityReading, HumidityReading]:
  override def processElement(value: HumidityReading,
                              ctx: ProcessFunction[HumidityReading, HumidityReading]#Context,
                              out: Collector[HumidityReading]
                             ): Unit =
    if value.humidity < 50 then ctx.output(FireAlert.lowHumidity, "Fire Hazard at " + value.location)
    else out.collect(value)

object FireAlert:
  lazy val lowHumidity = new OutputTag[String]("Fire Hazard")

@main def sideOutputDemo() =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityDataStream = inputFile.map(HumidityReading.fromString)

  val mainOutputStream = humidityDataStream.process(FireAlert())
  val sideOutputStream = mainOutputStream.getSideOutput(FireAlert.lowHumidity)

  mainOutputStream.print("Output Stream")
  sideOutputStream.print("Side Output Stream")

  env.execute()


