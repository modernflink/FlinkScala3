package com.example

import org.apache.flinkx.api.serializers.intInfo
import org.apache.flinkx.api.StreamExecutionEnvironment

object TestRun:
  def main(args: Array[String]): Unit =
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements(50 to 100: _*)
    data.print()
    env.execute()
