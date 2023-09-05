package modernflink.section1

import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.StreamExecutionEnvironment

@main def wordCount =

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val text = env.fromElements(
    "To be, or not to be,--that is the question:--",
    "Whether 'tis nobler in the mind to suffer",
    "The slings and arrows of outrageous fortune",
    "Or to take arms against a sea of troubles,"
  )

  text
    .flatMap(_.toLowerCase.split("\\W+"))
    .map((_, 1))
    .keyBy(_._1)
    .reduce((pair1, pair2) => (pair1._1, pair1._2 + pair2._2)).name("keyed sum")
    .print()

  env.execute("wordCount")