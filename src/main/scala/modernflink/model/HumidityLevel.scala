package modernflink.model

sealed trait HumidityLevel
case object AboveAverage extends HumidityLevel
case object Average extends HumidityLevel
case object BelowAverage extends HumidityLevel