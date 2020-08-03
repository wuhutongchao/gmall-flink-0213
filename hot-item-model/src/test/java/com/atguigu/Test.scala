package com.atguigu

import java.sql.Timestamp

object Test {

  def main(args: Array[String]): Unit = {

    val windowEnd: String = new Timestamp(1511658000 * 1000L).toString
    println(windowEnd)
  }

}
