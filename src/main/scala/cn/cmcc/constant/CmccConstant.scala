package cn.cmcc.constant

/**
  * Created by Huige 
  * Email: 824203453@qq.com 
  * DATE: 2019/2/21
  * Desc:
  * 保存常量数据
  * 枚举类型
  */
object CmccConstant extends Enumeration {

  val PROJECT = Value("cmcc")
  /* 封装了两个字段的名称*/
  val SERNAME = Value("serviceName")
  val RECHARGENR = Value("reChargeNotifyReq")

  /*day 截取的起始位置*/
  val DAYSTART = 0
  val DAYEND = 8

}
