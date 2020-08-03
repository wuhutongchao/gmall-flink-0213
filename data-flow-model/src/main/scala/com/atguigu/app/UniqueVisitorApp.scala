package com.atguigu.app

import java.sql.Timestamp

import com.atguigu.bean.{UserBehavior, UvCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UniqueVisitorApp {

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

    //6.开窗
    val windowAllDStream: AllWindowedStream[UserBehavior, TimeWindow] = pvDStream.timeWindowAll(Time.hours(1))

    //7.计算每个窗口中独立访客的数量
    //    val result: DataStream[UvCount] = windowAllDStream.apply(new UvAllWindowFunction)
    val result: DataStream[UvCount] = windowAllDStream.aggregate(new UvAgg, new UvAggWindowFunction)

    //8.打印
    result.print()

    //9.启动任务
    env.execute()

  }

}

class UvAllWindowFunction extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {

  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {

    //获取窗口结束时间
    val windowEnd: String = new Timestamp(window.getEnd).toString

    //去重
    var uids: Set[Long] = Set[Long]()
    input.foreach(userBehavior => {
      uids = uids + userBehavior.userId
    })

    //输出
    out.collect(UvCount(windowEnd, uids.size))
  }
}

//预聚合
class UvAgg extends AggregateFunction[UserBehavior, Set[Long], Long] {

  override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator + value.userId

  override def createAccumulator(): Set[Long] = Set[Long]()

  override def getResult(accumulator: Set[Long]): Long = accumulator.size

  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b
}

class UvAggWindowFunction extends AllWindowFunction[Long, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
    //获取窗口结束时间
    val windowEnd: String = new Timestamp(window.getEnd).toString

    //写出
    out.collect(UvCount(windowEnd, input.head))
  }
}