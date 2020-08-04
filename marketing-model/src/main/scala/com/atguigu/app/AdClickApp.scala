package com.atguigu.app

import java.sql.Timestamp

import com.atguigu.bean.{AdClickLog, BlackListWarning, CountByProvince}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object AdClickApp {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据
    val lineDStream: DataStream[String] = env.readTextFile("input/AdClickLog.csv")

    //3.转换为样例类并指定事件时间字段
    val adClickDStream: DataStream[AdClickLog] = lineDStream.map(line => {
      val arr: Array[String] = line.split(",")
      AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(line => {
      line.timestamp * 1000L
    })

    //黑名单过滤
    val filterByBlackList: DataStream[AdClickLog] = adClickDStream
      .keyBy(adClick => (adClick.userId, adClick.adId))
      .process(new FilterByBlackList)

    //4.分组
    val keyedDStream: KeyedStream[AdClickLog, String] = filterByBlackList.keyBy(_.province)

    //5.开窗
    val windowDStream: WindowedStream[AdClickLog, String, TimeWindow] = keyedDStream.timeWindow(Time.hours(1), Time.seconds(5))

    //6.计算
    val result: DataStream[CountByProvince] = windowDStream.aggregate(new AdClickAgg, new AdClickWindowFunction)

    //7.打印
    result.print()
    filterByBlackList.getSideOutput(new OutputTag[BlackListWarning]("warninng")).print("side")

    //8.执行
    env.execute()

  }

}


class AdClickAgg extends AggregateFunction[AdClickLog, Long, Long] {
  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdClickWindowFunction extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {

    val windowEnd: String = new Timestamp(window.getEnd).toString

    out.collect(CountByProvince(windowEnd, key, input.head))

  }
}

//定义状态保存数据,并过滤数据
class FilterByBlackList extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

  //定义保存单日用户对于某个广告点击次数的状态
  lazy val adClick: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("adClick", classOf[Long]))

  lazy val isSendWarning: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSendWarning", classOf[Boolean]))

  //定义状态保存定时器的时间
  lazy val time: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {

    val isSend: Boolean = isSendWarning.value()

    //如果从侧输出流输出过该用户信息,直接过滤掉
    if (isSend) {
      return
    }

    //定时器用于触发隔天凌晨删除状态的操作
    val ts: Long = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
    ctx.timerService().registerEventTimeTimer(ts)

    //获取状态信息
    val clickCount: Long = adClick.value()
    val curClickCount: Long = clickCount + 1

    //判读是否超过100次
    if (curClickCount < 100L) {
      //更新状态
      //adClick.update(clickCount)
      adClick.update(curClickCount)
      //正常输出数据
      out.collect(value)
    } else {

      //超过100次了,将数据写入侧输出流
      ctx.output(new OutputTag[BlackListWarning]("warninng"),
        BlackListWarning(value.userId,
          value.adId,
          "点击广告超过100次,注意！！！"))

      //更新状态
      isSendWarning.update(true)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {

    if (time.value() == timestamp) {
      println("********")
      //清空状态
      time.clear()
      adClick.clear()
      isSendWarning.clear()
    }

  }
}