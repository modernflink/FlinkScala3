package modernflink.section1

import org.apache.flink.streaming.api.*
import org.apache.flink.api.*
import org.apache.flink.api.serializers.*
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.io.Source
import modernflink.model.{HumidityReading, TemperatureReading}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream.Collector
import org.apache.flink.streaming.api.functions.{ProcessFunction}
import org.apache.flink.util

class FireAlert() extends ProcessFunction[HumidityReading, HumidityReading] {
  override def processElement(value: HumidityReading, ctx: ProcessFunction[HumidityReading, HumidityReading]#Context, out: util.Collector[HumidityReading]): Unit = {
    if value.humidity < 50 then {
      ctx.output(FireAlert.lowHumidity, "Fire Hazard at " + value.location)
    } else {
      out.collect(value)
    }
  }
}

object FireAlert {
  lazy val lowHumidity = new OutputTag[String]("Fire Hazard")
}

object SideOutput {

    def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
      val humidityDataStream = inputFile.map(HumidityReading.fromString)

      val outputProcessedStream = humidityDataStream.process(new FireAlert())
      val sideOutputStream = outputProcessedStream.getSideOutput(FireAlert.lowHumidity)

      outputProcessedStream.print("Output Stream")
      sideOutputStream.print("Side Output Stream")
      env.execute()
    }
  }
