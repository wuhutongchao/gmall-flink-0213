package com.atguigu.app

import com.atguigu.bean.{OrderEvent, OrderResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeOutWithProcess {

  def main(args: Array[String]): Unit = {

    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取文件数据
    //    val lineDStream: DataStream[String] = env.readTextFile("input/OrderLog.csv")
    val lineDStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //转换为样例类并指定数据中的时间字段
    val orderDStream: DataStream[OrderEvent] = lineDStream.map(line => {
      val arr: Array[String] = line.split(",")
      OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    }).assignAscendingTimestamps(_.eventTime * 1000L)

    //按照订单ID分组
    val keyedDStream: KeyedStream[OrderEvent, Long] = orderDStream.keyBy(_.orderId)

    //处理数据
    val result: DataStream[OrderResult] = keyedDStream.process(new OrderTimeOutProcessFunction)

    //打印
    result.print("Success")
    result.getSideOutput(new OutputTag[OrderResult]("TimeOut")).print("side")

    //执行
    env.execute()

  }

}

class OrderTimeOutProcessFunction extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  //定义一个状态用于存放定时器的时间
  lazy val time: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts", classOf[Long]))

  //每条数据单独处理
  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

    //判断是否为创建订单行为
    if (value.eventType == "create") {
      //注册定时器
      val ts: Long = value.eventTime * 1000L + 15 * 60 * 1000L + 5000L
      ctx.timerService().registerEventTimeTimer(ts)

      //更新状态
      time.update(ts)
    } else if (value.eventType == "pay") {

      //删除闹钟并输出数据
      ctx.timerService().deleteEventTimeTimer(time.value())
      time.clear()
      out.collect(OrderResult(value.orderId, "Success"))
    }
  }

  //定时器触发操作,订单超时
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {

    //输出数据至侧输出流
    val orderId: Long = ctx.getCurrentKey
    ctx.output(new OutputTag[OrderResult]("TimeOut"), OrderResult(orderId, "TimeOut"))

    //清空状态
    time.clear()
  }
}
