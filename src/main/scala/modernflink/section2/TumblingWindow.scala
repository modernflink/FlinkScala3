package modernflink.section2

import modernflink.model.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flinkx.api.serializers.*
import java.time.{Duration, Instant}

given instantTypeInfo: TypeInformation[Instant] =
  TypeInformation.of(classOf[Instant])

@main def tumblingWindowDemo() =

  val env = StreamExecutionEnvironment.getExecutionEnvironment

