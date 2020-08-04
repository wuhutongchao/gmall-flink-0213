package com.atguigu.bean

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
