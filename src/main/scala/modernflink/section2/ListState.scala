package modernflink.section2

import org.apache.flink.streaming.api.*
import org.apache.flink.api.StreamExecutionEnvironment
import org.apache.flink.api.serializers.*
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.io.Source
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.util.Collector

import java.time.{Duration, Instant}
import modernflink.model.{BankingEventGenerator, HumidityReading}
import modernflink.model.BankingEventGenerator.{Deposit, DepositEventGenerator}
import org.apache.flink.streaming.api.windowing.time.Time
import Given.given
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import collection.JavaConverters.iterableAsScalaIterableConverter

object ListState {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData = inputFile.map(HumidityReading.fromString)

  private def listStateDemo(): Unit = {
    // store all temperature change per location
    val humidityChangeStream = humidityData
      .keyBy(_.location)
      .process(new KeyedProcessFunction[String, HumidityReading, String] {
        // create state
        var humidityOutputStream: ListState[HumidityReading] = _

        //initialize state
        override def open(parameter: Configuration): Unit = {
          humidityOutputStream = getRuntimeContext.getListState(new ListStateDescriptor[HumidityReading]("humidityChangeOutputStream", classOf[HumidityReading]))
        }

        override def processElement(value: HumidityReading, ctx: KeyedProcessFunction[String, HumidityReading, String]#Context, out: Collector[String]): Unit = {

          humidityOutputStream.add(value)
          val humidityRecords: Iterable[HumidityReading] = humidityOutputStream.get().asScala.toList
          //keep the list bounded and clear the state when the condition is met
          if humidityRecords.size > 10 then
            humidityOutputStream.clear()

          out.collect(s"${value.location} - ${humidityRecords.mkString(",")}")
        }
      })
    humidityChangeStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    listStateDemo()
  }

}
