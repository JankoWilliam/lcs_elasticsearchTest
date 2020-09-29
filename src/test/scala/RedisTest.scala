import java.util

import cn.yintech.online.LiveVisitOnline.jsonParse
import cn.yintech.redisUtil.{RedisClientNew, RedisClientOld}

import scala.collection.mutable

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
    //    println(jedis.hgetAll("lcs:live:visit:count").size())
    //    println(jedis.hgetAll("lcs:live:visit:count:circle").size())
    import  scala.collection.JavaConverters._
    //    val set = jedis.smembers("lcs:live:visit:uid:62824").asScala
    //    println(jedis.keys("lcs:live:visit:uid:*"))
    //    println(jedis.scard("lcs:live:visit:uid:62824"))
    //    println(Set("1", "2") diff  set)
    var jsonStr = jedis.hget("lcs:live:visit:count", "21332")
    if (jsonStr == null || jsonStr == "" ) jsonStr = "{}"
    val json = jsonParse(jsonStr)
    val start_time = json.getOrElse("start_time","1970-01-01 08:00:00")
    val end_time = json.getOrElse("end_time","1970-01-01 08:00:00")
    val view_count = json.getOrElse("view_count","0")

    println(start_time)
    println(end_time)
    println(view_count)

    jedis.close()
//    jedis2.close()
  }
}
