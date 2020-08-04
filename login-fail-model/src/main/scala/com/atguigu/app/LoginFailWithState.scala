package com.atguigu.app

import java.{lang, util}

import com.atguigu.bean.{LoginEvent, LoginFailLog}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object LoginFailWithState {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据转换为样例类并指定时间戳
    val loginEventDStream: DataStream[LoginEvent] = env.readTextFile("input/LoginLog.csv")
      .map(line => {
        val arr: Array[String] = line.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
    })

    //3.分组
    val keyedDStream: KeyedStream[LoginEvent, Long] = loginEventDStream.keyBy(_.userId)

    //4.处理数据
    val result: DataStream[LoginFailLog] = keyedDStream.process(new LoginFailProcessFunction)

    result.print()

    env.execute()


  }

}

class LoginFailProcessFunction extends KeyedProcessFunction[Long, LoginEvent, LoginFailLog] {

  //定义状态用于存放单个用户登录失败的数据
  lazy val listState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("listState", classOf[LoginEvent]))

  //定义状态保存闹钟的触发时间
  lazy val ts: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailLog]#Context, out: Collector[LoginFailLog]): Unit = {

    //判断当前登录是否为失败
    if (value.eventType == "fail") {
      listState.add(value)

      //注册2秒后的闹钟
      if (ts.value() == 0) {
        val curTs: Long = value.eventTime * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(curTs)
        ts.update(curTs)
      }
    } else {
      //登录成功,则删除闹钟并清空状态
      ctx.timerService().deleteEventTimeTimer(ts.value())
      listState.clear()
      ts.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailLog]#OnTimerContext, out: Collector[LoginFailLog]): Unit = {

    //获取数据
    val events: util.Iterator[LoginEvent] = listState.get().iterator()

    //清空状态
    listState.clear()
    ts.clear()

    var count: Long = 0L
    var userId: Long = 0L
    while (events.hasNext) {
      count += 1
      userId = events.next().userId
    }

    if (count >= 2) {
      //登录失败超过2次
      out.collect(LoginFailLog(userId, count))
    }

  }
}