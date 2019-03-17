package cn.cmcc.kpianalysis

import java.util

import cn.cmcc.config.ConfigManager
import cn.cmcc.constant.CmccConstant
import cn.cmcc.utils.{DateUtils, RedisUtils}
import com.alibaba.fastjson.JSONObject
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import redis.clients.jedis.Jedis

/**
  * Created by Huige 
  * Email: 824203453@qq.com 
  * DATE: 2019/2/21
  * Desc:
  * 核心指标的业务逻辑
  */
object KpiAnalysis {

  /**
    *  提取所有的有用的字段
    * @param filterRDD  所有的充值通知的数据
    * @return  所有的包含有用字段的rdd
    */
   def makeBaseRDD(filterRDD: RDD[JSONObject]) = {
    filterRDD.map(json => {
      /* 是否充值成功*/
      val buss = json.getString("bussinessRst")
      /*业务流水号   20181012141516333xxxxx*/
      val requstId = json.getString("requestId")
      /*省份编码*/
      val procode = json.getString("provinceCode")

      /*获取天 小时  分钟 的时间*/
      val day = requstId.substring(CmccConstant.DAYSTART, CmccConstant.DAYEND)
      val hour = requstId.substring(8, 10)
      val minute = requstId.substring(10, 12)


      val kpi: (Int, Int, Double, Long) = if ("0000".equalsIgnoreCase(buss)) {
        /*金额*/
        val money = json.getDouble("chargefee")
        /*通知时间*/
        val notifyTime = json.getString("receiveNotifyTime")
        /* 截取时间戳*/
        val startTime = requstId.substring(0, 17)
        /*时间转换*/
        /*总次数  成功次数 money  充值时间*/
        (1, 1, money, DateUtils.calcTime(startTime, notifyTime))
      } else {
        (1, 0, 0.0D, 0L)
      }
      // 充值成功
      // day  hour  minute province  总次数  成功次数 money  充值时间
      (day, hour, minute, procode, kpi)
    }).cache()
  }


  /**
    *
    * @param baseRDD
    */
   def analysisRechargeKpi(baseRDD: RDD[(String, String, String, String, (Int, Int, Double, Long))]) = {
    /**
      * 业务概况  -- 统计天的充值成功的数量  成功金额  成功率  平均时长
      */
    baseRDD.map(tp => (tp._1, tp._5)).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
      .foreachPartition(it => {
        val jedis: Jedis = RedisUtils.getJedis()
        it.foreach {
          case (day, (total, success, money, times)) => {
            //  cmcc:day  "total"
            //  cmcc:dayhourminute:
            //  cmcc:dayhourminute:pro
            jedis.hincrBy(CmccConstant.PROJECT + ":" + day, "total", total)
            jedis.hincrBy(CmccConstant.PROJECT + ":" + day, "success", success)
            jedis.hincrByFloat(CmccConstant.PROJECT + ":" + day, "money", money)
            jedis.hincrBy(CmccConstant.PROJECT + ":" + day, "times", times)
          }
        }
        jedis.close()
      })
  }

  /**
    *  每小时的充值统计
    * @param baseRDD
    */
   def hourAnalysis(baseRDD: RDD[(String, String, String, String, (Int, Int, Double, Long))]) = {
    baseRDD.map(tp => ((tp._1, tp._2), (tp._5._1, tp._5._2))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .foreachPartition(it => {
        val jedis = RedisUtils.getJedis()
        it.foreach {
          case ((day, hour), (total, success)) => {
            jedis.hincrBy(CmccConstant.PROJECT + ":" + day, "total-" + hour, total)
            jedis.hincrBy(CmccConstant.PROJECT + ":" + day, "success-" + hour, success)
          }
        }
        jedis.close()
      })
  }
  /**
    *  每分钟的充值统计
    * @param baseRDD
    */
   def kpiMinuteAnalysis(baseRDD: RDD[(String, String, String, String, (Int, Int, Double, Long))]) = {
    baseRDD.map(tp => ((tp._1, tp._2, tp._3), (tp._5._2, tp._5._3))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .foreachPartition(it => {
        val jedis = RedisUtils.getJedis()
        it.foreach {
          case ((day, hour, minute), (success, money)) => {
            // 从广播变量中 获取 省份编码对应的省份名称
            jedis.hincrBy(CmccConstant.PROJECT + ":" + day + ":m", "s-" + hour + minute, success)
            jedis.hincrByFloat(CmccConstant.PROJECT + ":" + day + ":m", "m-" + hour + minute, money)
          }
        }
        jedis.close()
      })
  }

  /**
    *
    * @param ssc
    * @param baseRDD
    */
   def provinceAnalysis(ssc: StreamingContext, baseRDD: RDD[(String, String, String, String, (Int, Int, Double, Long))]) = {
    // 对省份的规则数据进行广播
    val bc: Broadcast[util.Map[String, AnyRef]] = ssc.sparkContext.broadcast(ConfigManager.pcodeAndPName)

    /**
      * 业务统计  -- 各省份  充值成功的数量   成功率
      */
    baseRDD.map(tp => ((tp._1, tp._4), (tp._5._1, tp._5._2))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .foreachPartition(it => {
        val jedis = RedisUtils.getJedis()
        // 需要从省份编码里面找出来 pcode < -->  province
        /*
           数据存mysql 数据存 redis  数据存hdfs   数据写入到配置文件中
           */
        it.foreach {
          case ((day, procode), (total, success)) => {
            // 从广播变量中 获取 省份编码对应的省份名称
            val province = bc.value.get(procode).asInstanceOf[String]
            jedis.hincrBy(CmccConstant.PROJECT + ":" + day + ":total", province, total)
            jedis.hincrBy(CmccConstant.PROJECT + ":" + day + ":success", province, success)

            /* 可以考虑把 各省份的成功订单数据 写入到zset中，*/
            jedis.zincrby(CmccConstant.PROJECT + ":" + day + ":success:z", success, province)

            /* 利用redis来查询 top10的数据
              jedis.zrevrange(CmccConstant.PROJECT + ":" + day + ":success",0,9)
              */
          }
        }
        jedis.close()
      })
  }
}
