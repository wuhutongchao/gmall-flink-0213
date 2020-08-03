package com.atguigu.app

import java.{lang, util}
import java.sql.Timestamp
import java.util.Map

import com.atguigu.bean.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItemApp02 {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.指定程序使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.读取文件数据创建流
    val lineDStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //4.将每一行数据转换为样例类对象并指定数据中的时间戳字段
    val userBehaviorDStream: DataStream[UserBehavior] = lineDStream.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
      override def extractTimestamp(element: UserBehavior): Long = {
        element.timestamp * 1000L
      }
    })

    //5.过滤出pv数据
    val pvDStream: DataStream[UserBehavior] = userBehaviorDStream.filter(_.behavior == "pv")

    //6.按照itemID分组
    val itemToUserBehaviorDStream: KeyedStream[UserBehavior, Long] = pvDStream.keyBy(_.itemId)

    //7.开窗:滑动窗口,窗口大小1小时,滑动步长5min
    val windowDStream: WindowedStream[UserBehavior, Long, TimeWindow] = itemToUserBehaviorDStream
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[UserBehavior]("lateData"))

    //全量窗口的方式
    //    val unit: DataStream[(String, Long, Long)] = windowDStream.apply(new HotItemWindowAllFunction2)
    //8.增量聚合的方式
    val itemViewCountDStream: DataStream[ItemViewCount] = windowDStream.aggregate(new HotItemAgg2, new HotItemWindowFunction2)

    //获取侧输出流
    val sideDStream: DataStream[UserBehavior] = itemViewCountDStream.getSideOutput(new OutputTag[UserBehavior]("lateData"))

    //9.按照窗口时间分组
    val windowEndToValueDStream: KeyedStream[ItemViewCount, Long] = itemViewCountDStream.keyBy(_.windowEnd)

    //10.对于同一个窗口中的数据排序,取TopN
    val result: DataStream[String] = windowEndToValueDStream.process(new HotItemKeyedProcessFunction2(5))

    //11.打印
    lineDStream.print("data")
    itemViewCountDStream.print("agg")
    result.print("result")
    sideDStream.print("side")

    //12.任务执行
    env.execute("HotItem TopN Job")

  }

}

//全量窗口的方式
class HotItemWindowAllFunction2 extends WindowFunction[UserBehavior, (String, Long, Long), Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[UserBehavior], out: Collector[(String, Long, Long)]): Unit = {
    val windowEnd: String = new Timestamp(window.getEnd).toString
    out.collect((windowEnd, key, input.size))
  }
}

//增量聚合的方式
class HotItemAgg2 extends AggregateFunction[UserBehavior, Long, Long] {
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//对于预聚合后的数据进行加工,输出带窗口信息的数据
class HotItemWindowFunction2 extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.head))
  }
}

class HotItemKeyedProcessFunction2(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  //创建一个ListState用于存放同一个Key(同一个窗口)下所有数据
  //  lazy val listState: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("listState", classOf[ItemViewCount]))

  //创建一个MapState用于存放同一个Key(同一个窗口)下所有数据
  lazy val mapState: MapState[Long, ItemViewCount] = getRuntimeContext.getMapState(new MapStateDescriptor[Long, ItemViewCount]("mapState", classOf[Long], classOf[ItemViewCount]))


  //来一条数据处理一条
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //添加元素
    mapState.put(value.itemId, value)
    //注册一个定时器用于处理同一个窗口收集完的数据
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L)

    //注册定时器用于控制清空状态时机
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L + 100L)
  }

  //闹钟响起的时候触发计算
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //清空状态
    if (timestamp == ctx.getCurrentKey + 60 * 1000L + 100L) {
      mapState.clear()
      return
    }

    //从状态中获取数据
    //    val itemViewCounts: lang.Iterable[ItemViewCount] = listState.get()
    val iterable: lang.Iterable[util.Map.Entry[Long, ItemViewCount]] = mapState.entries()

    //将itemViewCounts转换为List
    val listBuffer = new ListBuffer[ItemViewCount]
    import scala.collection.JavaConverters._
    for (entry <- iterable.asScala) {
      listBuffer += entry.getValue
    }

    //对listBuffer排序
    val topNList: List[ItemViewCount] = listBuffer.toList.sortWith(_.count > _.count).take(topSize)

    //将topNList转换为String进行输出
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- topNList.indices) {
      val currentItem: ItemViewCount = topNList(i)
      // e.g.  No1：  商品ID=12224  浏览量=2413
      result.append("No").append(i + 1).append(":")
        .append("  商品ID=").append(currentItem.itemId)
        .append("  浏览量=").append(currentItem.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)

    out.collect(result.toString)

  }

}