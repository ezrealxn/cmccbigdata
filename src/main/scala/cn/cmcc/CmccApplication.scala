package cn.cmcc

import java.text.SimpleDateFormat
import java.util

import cn.cmcc.config.ConfigManager
import cn.cmcc.constant.CmccConstant
import cn.cmcc.kpianalysis.KpiAnalysis
import cn.cmcc.offset.OffsetManager
import cn.cmcc.utils.RedisUtils
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Created by Huige
  * Email: 824203453@qq.com
  * DATE: 2019/2/21
  * Desc:
  * 实时监控平台的 主程序
  */
// 业务逻辑
/**
  * 需求分析：
  * 1，所有的指标都是充值相关的指标  ，而原始数据中，除了充值通知的数据之后，还有支付通知的数据
  *
  * 注意事项： 实际中，不同的日志，可能会分topic存储 日志数据是不是分开存储的？
  *
  * 需要从日志数据中，提取 充值相关的数据
  * serviceName reChargeNotifyReq
  *
  * 2，具体到涉及到哪些日志字段
  * 天
  * 充值成功： bussinessRst   次数  1 0     充值的总的订单次数  1
  * 充值金额   chargefee
  * 业务流水号：requestId
  * 响应时间： receiveNotifyTime
  *
  * 天 小时
  *
  * 天 省份   provinceCode
  *
  * 天 小时  分钟
  *
  * 计算指标：
  * 1，充值成功的数量  成功金额  成功率  平均时长
  * 2，每小时成功订单数量  成功率 = 成功订单数量 /  总的数量
  * 3，各省份  充值成功的数量   成功率  top10
  * 4，每分钟的充值金额  和 充值成功的数量
  */

class CmccApplication

object CmccApplication {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    /*
     spark conf
     streaming 的配置
     kafka 的配置
     Jedis连接
     */
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("实时监控指标分析")
//      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      // kryo 序列化机制
      .registerKryoClasses(Array(classOf[CmccApplication]))
      .set("spark.serializer", classOf[KryoSerializer].getName)
    /*
     上述配置参数，可以在SparkConf中写
     也可以在程序执行的时候，通过 --conf k=v
     */
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(ConfigManager.intervalTime))

    /*通过KfkaUtils 创建 DStream*/
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(ConfigManager.topic), ConfigManager.kafkaParams, OffsetManager.getOffsetsMap))

    dstream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //*获取偏移量*/
        val offsetRng: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        offsetRng.foreach(println)

        /* 解析json字符串
        * 并过滤出所有的充值通知的数据*/
        val filterRDD = rdd.map(str => JSON.parseObject(str.value()))
          //        .filter(json => json.getString("serviceName").equals("reChargeNotifyReq"))
          .filter(json => json.getString(CmccConstant.SERNAME.toString).equals(CmccConstant.RECHARGENR.toString))

        /* 提取 所有的有用的字段 */
        val baseRDD = KpiAnalysis.makeBaseRDD(filterRDD)

        /** 业务概况  -- 统计天的充值成功的数量  成功金额  成功率  平均时长 */
        KpiAnalysis.analysisRechargeKpi(baseRDD)

        /** 业务概况  -- 每小时成功订单数量  成功率 = 成功订单数量 /  总的数量 */
        KpiAnalysis.hourAnalysis(baseRDD)
        /*各省份的充值统计*/
        KpiAnalysis.provinceAnalysis(ssc, baseRDD)
        /* 每分钟的充值金额  和 充值成功的数量*/
        KpiAnalysis.kpiMinuteAnalysis(baseRDD)

        /*保存偏移量*/
        OffsetManager.commitOffset2Redis(offsetRng)
      }
    })

    // 启动
    ssc.start()
    ssc.awaitTermination()
  }

}
