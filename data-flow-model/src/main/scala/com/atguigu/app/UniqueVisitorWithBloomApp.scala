package com.atguigu.app

import java.lang

import com.atguigu.bean.{UserBehavior, UvCount}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UniqueVisitorWithBloomApp {

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
    val strToUserIdDStream: DataStream[(String, Long)] = pvDStream.map(line => ("uv", line.userId))
    val keyedDStream: KeyedStream[(String, Long), String] = strToUserIdDStream.keyBy(_._1)
    val result: DataStream[UvCount] = keyedDStream.timeWindow(Time.hours(1))
      .trigger(new MyTrigger)
      .process(new UvCountProcessFunction)

    //8.打印
    //    result.print()

    //9.启动任务
    env.execute()

  }
}

//自定义触发器,来一条数据触发一次计算
class MyTrigger extends Trigger[(String, Long), TimeWindow] {

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
}

//来一条数据就处理一条,将位图和每个窗口中的用户数量保存至Redis
class UvCountProcessFunction extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  //声明Redis连接
  var jedis: Jedis = _
  var bloom: Bloom = _
  //每个窗口保存用户数据量的状态
  //  var countState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("hadoop102", 6379)
    bloom = new Bloom(1 << 30)
    //    countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState", classOf[Long]))
  }

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {

    //定义Redis中保存用户量的RedisKey
    val uvCountKey = "uvCountKey"
    //    val count: Long = countState.value()

    //取出UserId
    val userId: Long = elements.head._2

    //计算Hash
    val offset: Long = bloom.hash(userId.toString, 31)

    //一个窗口保存一个位图信息
    val windowEnd: String = context.window.getEnd.toString

    //查看位图中对应位置是否存在数据
    val isExist: lang.Boolean = jedis.getbit(windowEnd, offset)

    //如果不存在
    if (!isExist) {
      //保存位图
      jedis.setbit(windowEnd, offset, true)
      //      val curCount: Long = count + 1
      //更新状态并写入Redis
      //      countState.update(curCount)
      jedis.hincrBy(uvCountKey, windowEnd, 1L)
    }

    //将数据写出
    //    out.collect(UvCount(windowEnd, jedis.hget(uvCountKey, windowEnd).toLong))

  }
}

class Bloom(size: Long) extends Serializable {

  //传入的位图大小最好为2的整次幂
  val cap: Long = size

  //Hash函数
  def hash(str: String, seed: Int): Long = {

    var result = 1L

    for (i <- 0 until str.length) {
      result = result * seed + str.charAt(i)
    }

    //取模
    (cap - 1) & result

  }

}