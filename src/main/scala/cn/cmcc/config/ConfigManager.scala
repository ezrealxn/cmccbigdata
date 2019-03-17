package cn.cmcc.config

import java.util

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable

/**
  * Created by Huige 
  * Email: 824203453@qq.com 
  * DATE: 2019/2/21
  * Desc: 
  */
object ConfigManager {

  /*  获取读取配置文件的对象 */
  private lazy val config: Config = ConfigFactory.load()

  val topic = config.getString("kafka.topic")
  /* 所有的broker节点 */
  private val _brokers: String = config.getString("kafka.brokerlist")
  /*group Id*/
   val groupId: String = config.getString("kafka.groupId")
  /* 读取kafka数据的位置*/
  private val reset: String = config.getString("kafka.auto.reset")
  /*是否自动维护偏移量*/
  private val commit: Boolean = config.getBoolean("kafka.enable.commit")

  // 通过方法 调用私有属性
  def brokers = _brokers

  // 读取kafka的配置参数
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupId,
    "auto.offset.reset" -> reset,
    "enable.auto.commit" -> commit.asInstanceOf[Object]
  )

  /*streaming 程序的批次时间*/
  val intervalTime = config.getInt("streaming.batch.time")


  /*redis的配置读取*/
  val redisHost: String = config.getString("redis.host")
  val redisPort = config.getInt("redis.port")

  /* 加载 省份对应的数据*/
  import scala.collection.JavaConversions._
  val pcodeAndPName= config.getObject("pcodeAndProName").unwrapped()


  def main(args: Array[String]): Unit = {
    val value: AnyRef = pcodeAndPName("891")
    println(pcodeAndPName("891").asInstanceOf[String])
  }
}
