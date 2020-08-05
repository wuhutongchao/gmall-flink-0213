package com.atguigu.app

import com.atguigu.bean.{OrderEvent, ReceiptEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object PayReceiptWithConnectApp {

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

    //连接两条流
    val connectDStream: ConnectedStreams[OrderEvent, ReceiptEvent] = txIDOrderDStream.connect(txIDReceiptDStream)

    //处理数据
    val result: DataStream[(OrderEvent, ReceiptEvent)] = connectDStream.process(new MyCoProcessFunction)

    //打印
    result.print("result")
    result.getSideOutput(new OutputTag[(String, ReceiptEvent)]("noMatchOrder")).print("noMatchOrder")
    result.getSideOutput(new OutputTag[(OrderEvent, String)]("noMatchReceipt")).print("noMatchReceipt")

    //执行
    env.execute()

  }

}

class MyCoProcessFunction extends KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

  //定义状态用于存放两条流里面的数据
  lazy val orderState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("OrderEventState", classOf[OrderEvent]))
  lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("ReceiptEventState", classOf[ReceiptEvent]))

  lazy val orderTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("order-time", classOf[Long]))
  lazy val receiptTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("receipt-time", classOf[Long]))

  //处理OrderEvent数据
  override def processElement1(value: OrderEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    //取出receiptState中的数据
    val receiptEvent: ReceiptEvent = receiptState.value()

    //判断receiptEvent是否为空
    if (receiptEvent != null) {

      //写出数据并清空状态
      out.collect((value, receiptEvent))
      receiptState.clear()

      //删除对面的定时器
      ctx.timerService().deleteEventTimeTimer(receiptTime.value())

    } else {
      //将自身保存至状态中并注册定时器
      orderState.update(value)
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
      orderTime.update(value.eventTime * 1000L + 5000L)
    }

  }

  //处理ReceiptEvent数据
  override def processElement2(value: ReceiptEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //取出orderState中的数据
    val orderEvent: OrderEvent = orderState.value()

    //判断receiptEvent是否为空
    if (orderEvent != null) {
      //写出数据并清空状态
      out.collect((orderEvent, value))
      orderState.clear()

      //删除对面定时器
      ctx.timerService().deleteEventTimeTimer(orderTime.value())

    } else {
      //将自身保存至状态中并注册定时器
      receiptState.update(value)
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)

      receiptTime.update(value.eventTime * 1000L + 5000L)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    //获取状态信息
    val orderEvent: OrderEvent = orderState.value()
    val receiptEvent: ReceiptEvent = receiptState.value()

    if (orderEvent == null) {
      ctx.output(new OutputTag[(String, ReceiptEvent)]("noMatchOrder"), ("没有支付数据", receiptEvent))
    }

    if (receiptEvent == null) {
      ctx.output(new OutputTag[(OrderEvent, String)]("noMatchReceipt"), (orderEvent, "没有到账数据"))
    }

    //清空状态
    orderState.clear()
    receiptState.clear()
    orderTime.clear()
    receiptTime.clear()
  }
}