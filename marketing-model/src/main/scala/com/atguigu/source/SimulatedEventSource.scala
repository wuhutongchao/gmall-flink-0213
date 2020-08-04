package com.atguigu.source

import java.util.concurrent.TimeUnit

import com.atguigu.bean.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior] {

  var running = true

  override def cancel(): Unit = running = false

  val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")

  val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")

  val rand: Random = Random

  override def run(ctx: SourceContext[MarketingUserBehavior]): Unit = {

    val maxElements: Long = Long.MaxValue
    var count = 0L

    while (running && count < maxElements) {

      val id: Long = rand.nextLong()
      val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts: Long = System.currentTimeMillis()

      ctx.collectWithTimestamp(MarketingUserBehavior(id, behaviorType, channel, ts), ts)
      count += 1
      TimeUnit.MILLISECONDS.sleep(5L)
    }
  }


}

