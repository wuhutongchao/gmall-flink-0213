package com.atguigu.app

import java.util

import com.atguigu.bean.{OrderEvent, OrderResult}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object OrderTimeOutApp {

  def main(args: Array[String]): Unit = {

    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取文件数据
    val lineDStream: DataStream[String] = env.readTextFile("input/OrderLog.csv")

    //转换为样例类并指定数据中的时间字段
    val orderDStream: DataStream[OrderEvent] = lineDStream.map(line => {
      val arr: Array[String] = line.split(",")
      OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    }).assignAscendingTimestamps(_.eventTime * 1000L)

    //按照订单ID分组
    val keyedDStream: KeyedStream[OrderEvent, Long] = orderDStream.keyBy(_.orderId)

    //1.定义事件模式
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("start").where(_.eventType == "create")
      .followedBy("followed").where(_.eventType == "pay")
      .within(Time.seconds(15 * 60 + 5))

    //2.将事件模式作用到流上
    val patternDStream: PatternStream[OrderEvent] = CEP.pattern(keyedDStream, pattern)

    //3.提取事件
    val result: DataStream[OrderResult] = patternDStream.select(
      new OutputTag[OrderResult]("timeout"),
      new OrderTimeOut,
      new OrderSuccessPay)

    //打印
    result.print("success")
    result.getSideOutput(OutputTag[OrderResult]("timeout")).print("Side")

    //启动
    env.execute()
  }
}

//处理超时事件
class OrderTimeOut extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val createEvent: OrderEvent = map.get("start").get(0)
    OrderResult(createEvent.orderId, "TimeOut")
  }
}

class OrderSuccessPay extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val createEvent: OrderEvent = map.get("start").get(0)
    OrderResult(createEvent.orderId, "Success")
  }
}
