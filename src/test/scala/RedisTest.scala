import java.util

import cn.yintech.redisUtil.{RedisClientNew, RedisClientOld}

object RedisTest {
  def main(args: Array[String]): Unit = {
    // redis连接初始化
    val jedis = RedisClientOld.pool.getResource
//    val jedis2 = RedisClientNew.pool.getResource //新redis

//    import scala.collection.JavaConverters._
//    val map = jedis2.hgetAll("lcs:live:visit:count").asScala
//    for((key,value) <- map){
//      jedis.hset("lcs:live:visit:count",key,value)
//    }
    println(jedis.hgetAll("lcs:live:visit:count").size())
    println(jedis.hgetAll("lcs:live:visit:count:circle").size())
    jedis.close()
//    jedis2.close()
  }
}
