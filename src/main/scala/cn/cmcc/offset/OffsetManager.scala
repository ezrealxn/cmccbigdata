package cn.cmcc.offset

import java.util

import cn.cmcc.config.ConfigManager
import cn.cmcc.utils.RedisUtils
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Created by Huige 
  * Email: 824203453@qq.com 
  * DATE: 2019/2/21
  * Desc: 
  */
object OffsetManager {
  /**
    * 从redis中获取偏移量
    */
  def getOffsetsMap = {
    val offsetsMap = mutable.HashMap[TopicPartition, Long]()
    val topic = ConfigManager.topic
    val jedis: Jedis = RedisUtils.getJedis()
    val offsetValues: util.Map[String, String] = jedis.hgetAll(topic + "-" + ConfigManager.groupId)
    import scala.collection.JavaConversions._
    // 循环赋值
    for (of <- offsetValues) {
      offsetsMap(new TopicPartition(topic, of._1.toInt)) = of._2.toLong
    }
    offsetsMap
  }

  /**
    * 提交偏移量到redis中
    *
    * @param offsetRng
    */
  def commitOffset2Redis(offsetRng: Array[OffsetRange]) = {
    val jedis = RedisUtils.getJedis()
    for (of <- offsetRng) {
      // topic-groupId  partition  offset
      val topicAndGroup = of.topic + "-" + ConfigManager.groupId
      jedis.hset(topicAndGroup, of.partition.toString, of.untilOffset.toString)
    }
    jedis.close()
  }
}
