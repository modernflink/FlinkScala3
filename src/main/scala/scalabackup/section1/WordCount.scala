import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.serializers.*

@main def wordCount =

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val text = env.fromElements(
    "To be, or not to be,--that is the question:--",
    "Whether 'tis nobler in the mind to suffer",
    "The slings and arrows of outrageous fortune",
    "Or to take arms against a sea of troubles,"
  )

  val wordCountOutput = text
    .flatMap(_.toLowerCase.split("\\W+"))
    .map((_, 1))
    .keyBy(_._1)
    .reduce((a, b) => (a._1, a._2 + b._2))

  wordCountOutput.print()
  env.execute()