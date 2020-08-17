package cn.yintech.online

import java.util

import cn.yintech.online.LiveVisitOnline.jsonParse
import cn.yintech.redisUtil.RedisClient
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

object RedisToHive {
  def main(args: Array[String]): Unit = {

    //  获取日期分区参数
    require(!(args == null || args.length != 1 ), "Required 'dt' arg")
    val pattern = new Regex("\\d{4}[-]\\d{2}[-]\\d{2}")
    val dateSome1 = pattern findFirstIn args(0)
    require(dateSome1.isDefined, s"Required PARTITION args like 'yyyy-mm-dd' but find ${args(0)}")
    val dt = dateSome1.get // 实际使用yyyy-mm-dd格式日期
    println("doing date dt : " + dt )

    val spark = SparkSession.builder()
//      .master("local")
      .appName("RedisToHive")
      .enableHiveSupport()
      .getOrCreate()


    import scala.collection.JavaConversions._
    val jedis = RedisClient.pool.getResource
    val redesHash1 = jedis.hgetAll("lcs:live:visit:count").toSeq
    val redesHash2 = jedis.hgetAll("lcs:live:visit:count:circle").toSeq
    import spark.implicits._
    // lcs:live:visit:count
    spark.sparkContext.parallelize(redesHash1)
      .map(v => {
        val dataMap = jsonParse(v._2)
        LcsLiveVisitCount(
          dataMap.getOrElse("live_id", ""),
          dataMap.getOrElse("circle_id", ""),
          dataMap.getOrElse("lcs_id", ""),
          dataMap.getOrElse("lcs_name", ""),
          dataMap.getOrElse("start_time", ""),
          dataMap.getOrElse("end_time", ""),
          dataMap.getOrElse("live_title", ""),
          dataMap.getOrElse("live_status", ""),
          dataMap.getOrElse("view_count", ""),
          dataMap.getOrElse("view_count_robot", ""),
          dataMap.getOrElse("first_view_count", ""),
          dataMap.getOrElse("max_living_count", ""),
          dataMap.getOrElse("new_follow_count", ""),
          dataMap.getOrElse("new_follow_count_robot", ""),
          dataMap.getOrElse("comment_count", ""),
          dataMap.getOrElse("comment_count_robot", ""),
          dataMap.getOrElse("comment_peoples", ""),
          dataMap.getOrElse("comment_peoples_robot", ""),
          dataMap.getOrElse("gift_count", ""),
          dataMap.getOrElse("gift_count_robot", ""),
          dataMap.getOrElse("share_count", ""),
          dataMap.getOrElse("share_count_robot", ""),
          dataMap.getOrElse("old_user_staying_average", ""),
          dataMap.getOrElse("old_user_staying_max", ""),
          dataMap.getOrElse("old_user_staying_min", ""),
          dataMap.getOrElse("new_user_staying_average", ""),
          dataMap.getOrElse("new_user_staying_max", ""),
          dataMap.getOrElse("new_user_staying_min", ""),
          dataMap.getOrElse("gift_income", "")
        )
      }).toDF()
      .createOrReplaceGlobalTempView("table1")

    spark.sql(s"insert overwrite table ads.ads_redis_lcs_live_visit_count_df partition(dt='$dt')" +
      " select * from global_temp.table1 ")
    // lcs:live:visit:count:circle
    spark.sparkContext.parallelize(redesHash2)
      .map(v => {
        val dataMap = jsonParse(v._2)
        (
          v._1,
          dataMap.getOrElse("notice_num", ""),
          dataMap.getOrElse("live_times", ""),
          dataMap.getOrElse("view_count", ""),
          dataMap.getOrElse("view_count_robot", ""),
          dataMap.getOrElse("max_living_count", ""),
          dataMap.getOrElse("comment_count", ""),
          dataMap.getOrElse("comment_count_robot", ""),
          dataMap.getOrElse("comment_peoples", ""),
          dataMap.getOrElse("comment_peoples_robot", ""),
          dataMap.getOrElse("gift_income", "")
        )
      }).toDF(
      "lcs_id",
      "notice_num",
      "live_times",
      "view_count",
      "view_count_robot",
      "max_living_count",
      "comment_count",
      "comment_count_robot",
      "comment_peoples",
      "comment_peoples_robot",
      "gift_income"
    )
      .createOrReplaceGlobalTempView("table2")

    spark.sql(s"insert overwrite table ads.ads_redis_lcs_live_visit_count_circle_df partition(dt='$dt')" +
      " select * from global_temp.table2 ")

    spark.stop()

  }

  case class LcsLiveVisitCount(
                                live_id: String,
                                circle_id: String,
                                lcs_id: String,
                                lcs_name: String,
                                start_time: String,
                                end_time: String,
                                live_title: String,
                                live_status: String,
                                view_count: String,
                                view_count_robot: String,
                                first_view_count: String,
                                max_living_count: String,
                                new_follow_count: String,
                                new_follow_count_robot: String,
                                comment_count: String,
                                comment_count_robot: String,
                                comment_peoples: String,
                                comment_peoples_robot: String,
                                gift_count: String,
                                gift_count_robot: String,
                                share_count: String,
                                share_count_robot: String,
                                old_user_staying_average: String,
                                old_user_staying_max: String,
                                old_user_staying_min: String,
                                new_user_staying_average: String,
                                new_user_staying_max: String,
                                new_user_staying_min: String,
                                gift_income: String
                              )
}
