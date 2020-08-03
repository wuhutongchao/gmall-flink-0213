package com.atguigu.app

import com.atguigu.bean.{PvCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object PvApp {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.指定程序使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.读取文件数据创建流
    val lineDStream: DataStream[String] = env.readTextFile("input/UserBehavior.csv")

    //4.将每一行数据转换为样例类对象并指定数据中的时间戳字段
    val userBehaviorDStream: DataStream[UserBehavior] = lineDStream.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    //5.过滤出pv数据
    val pvDStream: DataStream[UserBehavior] = userBehaviorDStream.filter(_.behavior == "pv")

    //6.分组
    val pvToOneDStream: DataStream[(String, Long)] = pvDStream.map(line => ((Random.nextInt(8) + 1).toString, 1L))
    val keyedDStream: KeyedStream[(String, Long), String] = pvToOneDStream.keyBy(_._1)

    //7.开窗
    val windowDStream: WindowedStream[(String, Long), String, TimeWindow] = keyedDStream.timeWindow(Time.hours(1))

    //8.聚合
    val pvCountDStream: DataStream[PvCount] = windowDStream.aggregate(new PvAgg, new PvResultWindowFunction)

    //9.打印
    //pvCountDStream.print()
    //重新按照窗口时间进行分组
    val windowEndToPvDStream: KeyedStream[PvCount, Long] = pvCountDStream.keyBy(_.windowEnd)

    //分组之后重新聚合
    val result: DataStream[PvCount] = windowEndToPvDStream.process(new PvProcessFunction)

    result.print()

    //10.启动任务
    env.execute("PvCount Job")

  }

}

//预聚合每个窗口中的PV数据
class PvAgg extends AggregateFunction[(String, Long), Long, Long] {
  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//对预聚合之后的数据进行再次加工
class PvResultWindowFunction extends WindowFunction[Long, PvCount, String, TimeWindow] {

  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.head))
  }
}


class PvProcessFunction extends KeyedProcessFunction[Long, PvCount, PvCount] {

  //定义状态用于存放每个窗口中PV数据
  lazy val pvState: ValueState[PvCount] = getRuntimeContext.getState(new ValueStateDescriptor[PvCount]("pvState", classOf[PvCount]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
    val pvCount: PvCount = pvState.value()
    if (pvCount == null) {
      pvState.update(value)
    } else {
      pvState.update(PvCount(pvCount.windowEnd, pvCount.count + value.count))
    }

    //创建一个定时器定时触发计算输出结果
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L)
  }

  //定时器触发的操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {

    //获取状态
    val pvCount: PvCount = pvState.value()

    //清空状态
    pvState.clear()

    //将数据写出
    out.collect(pvCount)
  }
}