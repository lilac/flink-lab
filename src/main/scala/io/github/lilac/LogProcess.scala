package io.github.lilac

/**
 * Copyright SameMo 2020
 */

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.assigners.{ GlobalWindows, TumblingEventTimeWindows }
import org.apache.flink.streaming.api.windowing.time.Time

object LogProcess {
  def main(args: Array[String]): Unit = {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val logs = env.fromElements(
      RawLog("race-1", 1, "start", children = List("message-1", "message-2")),
      RawLog("message-1", 2, "click", List.empty),
      RawLog("message-2", 3, "click", List.empty),
      RawLog("message-1", 4, "click", List.empty),
      RawLog("message-1", 5, "click", List.empty),
      RawLog("message-1", 6, "click", List.empty),
    ).assignAscendingTimestamps(_.time)

    val startEvents = logs.filter { log =>
      log.action == "start"
    }.flatMap { log =>
      for (c <- log.children) yield FlatLog(log.id, log.time, log.action, c)
    }
    val feedbackEvents = logs.filter { log => log.action == "click" }
    val sessionEvents = startEvents.join(feedbackEvents)
      .where(log => log.child)
      .equalTo(log => log.id)
      .window(TumblingEventTimeWindows.of(Time.minutes(30)))
//      .window(GlobalWindows.create())
      .apply { (left, right) => Log(right.id, right.time, left.id, right.action) }

    val clicks = sessionEvents
      .map {log => (log, 1)}
      .keyBy(_._1.id)
      .timeWindow(Time.minutes(30))
    .sum(1)

    sessionEvents.print("logs")
    clicks.print("clicks")

    env.execute("Log process")
  }

  type Timestamp = Long

  class BaseLog(id: String, time: Timestamp)

  case class RawLog(
                     id: String, time: Timestamp,
                     action: String, children: List[String])
    extends BaseLog(id, time)

  case class FlatLog(id: String, time: Timestamp,
                     action: String, child: String) extends BaseLog(id, time)

  case class Log(id: String, time: Timestamp,
                 sessionId: String, action: String) extends BaseLog(id, time)

}

