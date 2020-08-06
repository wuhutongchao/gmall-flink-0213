package com.atguigu.app

import com.atguigu.bean.{OrderEvent, ReceiptEvent}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object PayReceiptWithJoinApp {

  def main(args: Array[String]): Unit = {

    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取文件数据
    val orderOriDStream: DataStream[String] = env.readTextFile("input/OrderLog.csv")
    val receiptOriDStream: DataStream[String] = env.readTextFile("input/ReceiptLog.csv")

    //转换为样例类并指定数据中的时间字段
    val orderDStream: DataStream[OrderEvent] = orderOriDStream.map(line => {
      val arr: Array[String] = line.split(",")
      OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    }).assignAscendingTimestamps(_.eventTime * 1000L)

    val receiptDStream: DataStream[ReceiptEvent] = receiptOriDStream.map(line => {
      val arr: Array[String] = line.split(",")
      ReceiptEvent(arr(0), arr(1), arr(2).toLong)
    }).assignAscendingTimestamps(_.eventTime * 1000L)

    //过滤出支付数据
    val payDStream: DataStream[OrderEvent] = orderDStream.filter(_.txId != "")

    //按照事务ID分组
    val txIDOrderDStream: KeyedStream[OrderEvent, String] = payDStream.keyBy(_.txId)
    val txIDReceiptDStream: KeyedStream[ReceiptEvent, String] = receiptDStream.keyBy(_.txId)

    //双流JOIN
    val result: DataStream[(OrderEvent, ReceiptEvent)] = txIDOrderDStream.intervalJoin(txIDReceiptDStream)
      .between(Time.hours(-15), Time.hours(15))
      .process(new MyProcessJoinFunction)

    //打印
    result.print()

    //执行
    env.execute()

  }

}


class MyProcessJoinFunction extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    out.collect((left, right))

  }
}