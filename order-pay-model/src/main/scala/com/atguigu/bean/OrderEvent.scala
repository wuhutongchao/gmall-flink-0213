package com.atguigu.bean

case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)
