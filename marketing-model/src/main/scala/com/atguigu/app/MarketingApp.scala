package com.atguigu.app

import com.atguigu.bean.{MarketingCountView, MarketingUserBehavior}
import com.atguigu.source.SimulatedEventSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MarketingApp {

  def main(args: Array[String]): Unit = {

    //1.获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.加载数据
    val marketingUserBehaviorDStream: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource)

    //3.过滤掉卸载行为
    val filterDStream: DataStream[MarketingUserBehavior] = marketingUserBehaviorDStream.filter(_.behavior != "UNINSTALL")

    //4.按照渠道和行为分组
    val keyedDStream: KeyedStream[MarketingUserBehavior, (String, String)] = filterDStream.keyBy(x => (x.channel, x.behavior))

    //5.开窗
    val windowDStream: WindowedStream[MarketingUserBehavior, (String, String), TimeWindow] = keyedDStream.timeWindow(Time.hours(1), Time.seconds(1))

    //6.计算总和
    val result: DataStream[MarketingCountView] = windowDStream.aggregate(new MarketAgg, new MarketWindowFunction)

    //7.打印
    result.print()

    //8.启动任务
    env.execute()
  }

}

//预聚合的类
class MarketAgg extends AggregateFunction[MarketingUserBehavior, Long, Long] {
  override def add(value: MarketingUserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class MarketWindowFunction extends WindowFunction[Long, MarketingCountView, (String, String), TimeWindow] {

  override def apply(key: (String, String), window: TimeWindow, input: Iterable[Long], out: Collector[MarketingCountView]): Unit = {
    out.collect(MarketingCountView(window.getStart, window.getEnd, key._1, key._2, input.head))
  }
}