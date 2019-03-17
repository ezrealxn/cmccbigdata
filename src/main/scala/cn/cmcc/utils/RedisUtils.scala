package cn.cmcc.utils

import cn.cmcc.config.ConfigManager
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by Huige 
  * Email: 824203453@qq.com 
  * DATE: 2019/2/21
  * Desc: 
  */
object RedisUtils {

  // 连接池的参数配置
  val config = new JedisPoolConfig()
  config.setMaxTotal(2000)
  // 最大连接数
  // 连接池
  private lazy val pool: JedisPool = new JedisPool(config, ConfigManager.redisHost, ConfigManager.redisPort)

  /*获取一个jedis连接*/
  def getJedis() = pool.getResource
}
