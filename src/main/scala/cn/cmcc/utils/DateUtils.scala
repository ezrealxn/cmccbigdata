package cn.cmcc.utils

import org.apache.commons.lang3.time.FastDateFormat


/**
  * Created by Huige 
  * Email: 824203453@qq.com 
  * DATE: 2019/2/21
  * Desc:
  * 时间的工具类
  */
object DateUtils {


  // 不是现线程安全的实例对象
  //  lazy val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  // 线程安全的时间操作对象  注意导包 import org.apache.commons.lang3.time.FastDateFormat
  lazy val sdf: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")

  def calcTime(start: String, end: String): Long = {
    /* 充值时长*/
    sdf.parse(end).getTime - sdf.parse(start).getTime
  }

}
