package com.atguigu.app

import java.util

import com.atguigu.bean.LoginEvent
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCep {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据转换为样例类并指定时间戳
    val loginEventDStream: DataStream[LoginEvent] = env.readTextFile("input/LoginLog.csv")
      .map(line => {
        val arr: Array[String] = line.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
    })

    //3.分组
    val keyedDStream: KeyedStream[LoginEvent, Long] = loginEventDStream.keyBy(_.userId)

    //4.定义事件模式
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("start").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail").times(3)
      .within(Time.seconds(5))

    //5.检测事件:将事件模式应用于流上
    val patternDStream: PatternStream[LoginEvent] = CEP.pattern(keyedDStream, pattern)

    //6.提取匹配到的事件
    //    val result: DataStream[(Long, String)] = patternDStream.select(new MyPatternSelectFunction)
    val result: DataStream[(Long, String)] = patternDStream.select(new OutputTag[(Long, String)]("timeOut"), new TimeOutFunction, new MyPatternSelectFunction)

    //7.打印
    result.print("result")
    result.getSideOutput(new OutputTag[(Long, String)]("timeOut")).print("side")

    //8.启动任务
    env.execute()

  }

}

class MyPatternSelectFunction extends PatternSelectFunction[LoginEvent, (Long, String)] {

  override def select(map: util.Map[String, util.List[LoginEvent]]): (Long, String) = {

    //提取事件
    val startEvent: LoginEvent = map.get("start").get(0)
    val nextEvent: LoginEvent = map.get("next").get(0)
    println(startEvent)
    println(nextEvent)

    //输出
    (startEvent.userId, s"在${startEvent.eventTime}到${nextEvent.eventTime}之间登录失败2次")

  }
}

//超时事件的提取
class TimeOutFunction extends PatternTimeoutFunction[LoginEvent, (Long, String)] {
  override def timeout(map: util.Map[String, util.List[LoginEvent]], timeoutTimestamp: Long): (Long, String) = {
    val event: LoginEvent = map.get("start").get(0)
    (event.userId, "TimeOut")
  }
}
