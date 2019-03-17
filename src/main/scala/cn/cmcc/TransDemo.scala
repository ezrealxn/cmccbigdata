package cn.cmcc

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}

import scala.collection.mutable

/**
  * Created by Huge 
  * 824203453@qq.com 
  * DATE: 2018/11/3
  * Desc: 
  */

object TransDemo {

  Logger.getLogger("org.apache").setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")


    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))

    val topics = Array("test004")
    val groupId = "xxxxxxxx"

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hdp-01:9092,hdp-02:9092,hdp-03:9092",
      "key.deserializer" -> classOf[StringDeserializer].getName,
      "value.deserializer" -> classOf[StringDeserializer].getName,
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false" // 手动维护偏移量
    )

    val offsetMap = mutable.HashMap[TopicPartition, Long]()

    // 需要读取数据库的操作
//    DBs.setup()
    val config = ConfigFactory.load()

    DB.readOnly { implicit session =>
      SQL(s"select * from ${config.getString("db.ostable")} where groupId = ?").bind(groupId).map(rs => {
        val topic = rs.get[String]("topic")
        val part = rs.get[Int]("part")
        val os = rs.get[Int]("offset")

        val tp = new TopicPartition(topic, part)
        offsetMap(tp) = os
      }).list().apply()
    }
    println("size=========" + offsetMap.size)
    //  消费kafka
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,

      LocationStrategies.PreferConsistent,
      // 订阅主题
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsetMap))

    ds.foreachRDD(rdd => {
      // Driver
      val getos: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(it => {
        // 在executor中做事务控制
        val data: Iterator[String] = it.map(_.value())
        // 当前task的id
        /*
            val pid = TaskContext.getPartitionId()
        // 获取当前处理数据所在的分区的offset
        */
        val pos = getos(TaskContext.get.partitionId)

        DB.localTx { implicit session =>
          data.foreach(w => {
            // 写入到mysql   模拟业务逻辑
            SQL(s"insert into t_wc values(?,?)").bind(w, 1).execute().apply()
          })
          // 写入偏移量到mysql  维护偏移量
   /*       SQL(s"update ${config.getString("db.ostable")} set offset = ?  where topic=? and part=? and groupId=?")
            .bind(pos.untilOffset, pos.topic, pos.partition, groupId).update().apply()*/

          SQL(s"replace into ${config.getString("db.ostable")} values(?,?,?,?) ").bind(pos.topic, pos.partition, pos.untilOffset,
            groupId).execute().apply()
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
