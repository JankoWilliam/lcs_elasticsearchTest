package cn.yintech.redisUtil

import java.io.FileInputStream
import java.util.Properties

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisClientOld extends Serializable {

  val redisHost: String = "47.104.254.17"
  val redisPort: Int = 9701
  val redisTimeout: Int = 30000
  val password: String = "fG2@bE1^hE4["


//  val redisHost = ""
//    val redisPort = 0
//    val redisTimeout = 0
//    val password = ""

  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout ,password)
 
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}