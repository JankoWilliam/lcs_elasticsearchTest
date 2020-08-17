package cn.yintech.online

import java.text.SimpleDateFormat

import cn.yintech.esUtil.ESConfigLcsTest
import cn.yintech.hbaseUtil.HbaseUtils
import cn.yintech.hbaseUtil.HbaseUtils.{getRow, setRow}
import cn.yintech.online.LiveVisitOnline.{jsonParse, readComment, readGiftIncome, readNoticeId}
import cn.yintech.redisUtil.{RedisClientTest, RedisClientNew}
import net.minidev.json.JSONObject
import org.apache.hadoop.hbase.TableName
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

/**
 *  直播间统计离线部分
 */
object LiveVisitOfflineTest {
  def main(args: Array[String]): Unit = {

    //saprk切入点
    val spark = SparkSession.builder()
//      .master("local")
      .appName("LiveVisitOffline")
      .enableHiveSupport()
      .getOrCreate()


    //  获取日期分区参数
    require(!(args == null || args.length != 2 ), "Required 'startDt & endDt' args")
    val pattern = new Regex("\\d{4}[-]\\d{2}[-]\\d{2}")
    val dateSome1 = pattern findFirstIn args(0)
    val dateSome2 = pattern findFirstIn args(1)
    require(dateSome1.isDefined, s"Required PARTITION args like 'yyyy-mm-dd' but find ${args(0)}")
    require(dateSome2.isDefined, s"Required PARTITION args like 'yyyy-mm-dd' but find ${args(1)}")
    val startDt = dateSome1.get // 实际使用yyyy-mm-dd格式日期
    val endDt = dateSome2.get // 实际使用yyyy-mm-dd格式日期
    println("startDate dt : " + startDt + ",endDate dt : "+endDt)

    val lineDataSet = spark.read.format("jdbc")
      //      .option("url", "jdbc:mysql://rm-2zebtm824um01072v5o.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false")
      .option("url", "jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "licaishi_w")
      .option("password", "a222541420a50a5")
      .option("dbtable", "lcs_circle_notice")
      .load()
    lineDataSet.createOrReplaceTempView("lcs_circle_notice")
    import spark.implicits._
    spark.sql(s"select id,circle_id,u_type,uid,title,cast(start_time as String) start_time,cast(end_time as String) end_time , live_status from lcs_circle_notice where id in (60829,60832)  and live_status in (0,1) AND (`u_type`=2) AND (`type`=4) AND (`audit`=1) AND (`status`=0) order by id")
      .map(row => (row.getLong(0).toString, row.getLong(1).toString, row.getInt(2).toString, row.getDecimal(3).toString, row.getString(4), row.getString(5), row.getString(6), row.getInt(7).toString))
      .filter(v => v._8 == "0" || v._8 == "1").repartition(20)
      .mapPartitions(r => {
        // hbase连接初始化
        val conn = HbaseUtils.getConnection
        val table = TableName.valueOf("user_live_visit_lcs")
        val htable = conn.getTable(table)
        val table2 = TableName.valueOf("lcs_live_visit_new_user")
        val htable2 = conn.getTable(table2)
        // redis连接初始化
        val jedis = RedisClientTest.pool.getResource
//        val jedis2 = RedisClientNew.pool.getResource
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val result = r.map(v => {
          import scala.collection.JavaConversions._
          // redis上一次数据
          val oldValue = jedis.hget("lcs:live:visit:count", v._1)
          var old_max_living_count = 0 // 前次最高在线人数
          var old_first_view_count = 0 // 前次首次观看人数
          if (oldValue != null) {
            old_max_living_count = jsonParse(oldValue).getOrDefault("max_living_count", "0").toInt
            old_first_view_count = jsonParse(oldValue).getOrDefault("first_view_count", "0").toInt
          }
          var end = v._7
//          if (v._8 == "1")
//            end = sdf.format(new Date())
          // 评论表
          val comment = readComment(v._2, v._6, end) // u_type,uid,discussion_type,is_robot
          val commentGroup = comment.map(v => (v.head, v(1), v(2), v(3))).groupBy(_._3)
          // ES数据用户观看记录
          println("ES searchOnlineAgg" + v._2, v._6, end)
          val onlineFromEs = ESConfigLcsTest.searchOnlineAgg(v._2, v._6, end)
          val onlineFromEsList = onlineFromEs.map(v => {
            val json = jsonParse(v)
            (
              json.getOrElse("extra_id", ""),
              json.getOrElse("type", ""),
              json.getOrElse("uid", ""),
              json.getOrElse("points", "").toInt * 30,
              json.getOrElse("device_id", "")
            )
          }).filter(v => v._2 == "圈子视频直播" && v._5.length > 0)
          // ES实时数据
//          val onlineFromEsCurrent = ESConfigLcsTest.searchOnlineAgg(v._2, "now-60s", "now")


          // 新用户
          val newUserFilter = onlineFromEsList.filter(a => {
            if (a._3.length.==(0)) {
              false
              // rowKey长度为0直接返回false
            }
            else {
              val hbaseList = getRow(htable, a._3.reverse)
              val hbaseList2 = getRow(htable2, v._1.reverse)
              val newUsers = if (hbaseList2.nonEmpty) hbaseList2.get(0) else "0"
              if (hbaseList.nonEmpty) {
                val extraIds = hbaseList.get(0) // 该用户观看过的圈子id连接字符串
                if (extraIds.contains(a._1)) {
                  false // 用户已观看列表包含该extraId，返回false
                }
                else {
                  setRow(htable, a._3.reverse, "cf1", "extraIds", extraIds + "-" + a._1) // 用户未观看过，加上该圈子id
                  setRow(htable2, v._1.reverse, "cf1", "userIds", newUsers + "-" + a._3)
                  true // 用户未观看，返回true
                }
              }
              else { // 用户已观看列表为空，返回true
                setRow(htable, a._3.reverse, "cf1", "extraIds", a._1) // 用户未观看过，加上该圈子id
                setRow(htable2, v._1.reverse, "cf1", "userIds", newUsers + "-" + a._3)
                true
              }
            }
          })
          val hbaseAllNewUsers = getRow(htable2, v._1.reverse)
          val allNewUsers = if (hbaseAllNewUsers.nonEmpty) hbaseAllNewUsers.get(0) else "0"
          // 老用户
          val oldUser = onlineFromEsList.filter(v => !allNewUsers.contains(v._3))
          // 新用户
          val newUser = onlineFromEsList.filter(v => allNewUsers.contains(v._3) &&  v._3.length > 0)
          // 结果字段
          val live_id = v._1
          val circle_id = v._2
          val lcs_id = v._4
          val lcs_name = "#"
          val start_time = v._6
          val end_time = v._7
          val live_title = v._5
          val live_status = v._8
          val view_count = onlineFromEsList.map(_._5).distinct.size
          val view_count_robot = comment.count(_ (3) == "1")
          val first_view_count = newUser.map(_._3).distinct.size
          val max_living_count = math.max(math.ceil(onlineFromEsList.size * 0.13).toInt, old_max_living_count)
//          val max_living_count = math.ceil(onlineFromEsList.size * 0.13).toInt

          val new_follow_count = commentGroup.getOrDefault("14", List()).count(_._4 == "0")
          val new_follow_count_robot = commentGroup.getOrDefault("14", List()).count(_._4 == "1")
          val comment_count = commentGroup.getOrDefault("0", List()).count(_._4 == "0")
          val comment_count_robot = commentGroup.getOrDefault("0", List()).count(_._4 == "1")
          val comment_peoples = commentGroup.getOrDefault("0", List()).filter(v => v._2 != "" && v._4 == "0").map(_._2).toSet.size
          val comment_peoples_robot = commentGroup.getOrDefault("0", List()).filter(v => v._2 != "" && v._4 == "1").map(_._2).toSet.size
          val gift_count = commentGroup.getOrDefault("8", List()).count(_._4 == "0")
          val gift_count_robot = commentGroup.getOrDefault("8", List()).count(_._4 == "1")
          val share_count = commentGroup.getOrDefault("2", List()).count(_._4 == "0") +
            commentGroup.getOrDefault("12", List()).count(_._4 == "0") +
            commentGroup.getOrDefault("13", List()).count(_._4 == "0")
          val share_count_robot = commentGroup.getOrDefault("2", List()).count(_._4 == "1") +
            commentGroup.getOrDefault("12", List()).count(_._4 == "1") +
            commentGroup.getOrDefault("13", List()).count(_._4 == "1")

          val old_user_staying_average =
            math.min((if(oldUser.isEmpty) 0 else oldUser.map(_._4).sum * 1.0 / oldUser.size),(sdf.parse(end).getTime-sdf.parse(v._6).getTime)/1000 )
          val old_user_staying_max =
            math.min((if (oldUser.isEmpty) 0 else oldUser.maxBy(_._4)._4),(sdf.parse(end).getTime - sdf.parse(v._6).getTime) / 1000)
          val old_user_staying_min =
            math.min((if (oldUser.isEmpty) 0 else oldUser.minBy(_._4)._4),(sdf.parse(end).getTime - sdf.parse(v._6).getTime) / 1000)
          val new_user_staying_average =
            math.min((if (newUser.isEmpty) 0 else newUser.map(_._4).sum * 1.0 / newUser.size),(sdf.parse(end).getTime - sdf.parse(v._6).getTime) / 1000)
          val new_user_staying_max =
            math.min((if (newUser.isEmpty) 0 else newUser.maxBy(_._4)._4),(sdf.parse(end).getTime - sdf.parse(v._6).getTime) / 1000)
          val new_user_staying_min =
            math.min((if (newUser.isEmpty) 0 else newUser.minBy(_._4)._4),(sdf.parse(end).getTime - sdf.parse(v._6).getTime) / 1000)
          val gift_income = readGiftIncome(lcs_id,start_time,end_time)

          val jsonObj = new JSONObject
          jsonObj.put("live_id", live_id)
          jsonObj.put("circle_id", circle_id)
          jsonObj.put("lcs_id", lcs_id)
          jsonObj.put("lcs_name", lcs_name)
          jsonObj.put("start_time", start_time)
          jsonObj.put("end_time", end_time)
          jsonObj.put("live_title", live_title)
          jsonObj.put("live_status", live_status)
          jsonObj.put("view_count", view_count + "")
          jsonObj.put("view_count_robot", view_count_robot + "")
          jsonObj.put("first_view_count", first_view_count + "")
          jsonObj.put("max_living_count", max_living_count + "")
          jsonObj.put("new_follow_count", new_follow_count + "")
          jsonObj.put("new_follow_count_robot", new_follow_count_robot + "")
          jsonObj.put("comment_count", comment_count + "")
          jsonObj.put("comment_count_robot", comment_count_robot + "")
          jsonObj.put("comment_peoples", comment_peoples + "")
          jsonObj.put("comment_peoples_robot", comment_peoples_robot + "")
          jsonObj.put("gift_count", gift_count + "")
          jsonObj.put("gift_count_robot", gift_count_robot + "")
          jsonObj.put("share_count", share_count + "")
          jsonObj.put("share_count_robot", share_count_robot + "")
          jsonObj.put("old_user_staying_average", old_user_staying_average + "")
          jsonObj.put("old_user_staying_max", old_user_staying_max + "")
          jsonObj.put("old_user_staying_min", old_user_staying_min + "")
          jsonObj.put("new_user_staying_average", new_user_staying_average + "")
          jsonObj.put("new_user_staying_max", new_user_staying_max + "")
          jsonObj.put("new_user_staying_min", new_user_staying_min + "")
          jsonObj.put("gift_income", gift_income + "")
          // 直播场次维度结果
          jedis.hset("lcs:live:visit:count",v._1,jsonObj.toJSONString())
//          jedis2.hset("lcs:live:visit:count", live_id, jsonObj.toJSONString())
          println(jsonObj.toJSONString())
          // 理财师维度结果
          val lcsCountResultList = readNoticeId(lcs_id).map( v => {
            var jsonStr = jedis.hget("lcs:live:visit:count",v)
            if (jsonStr == "")
              jsonStr = "{}"
            var json: Map[String, String] = Map()
            try{
              json = jsonParse(jsonStr)
            } catch {
              case e:Exception => {
                println("error str:" + jsonStr )
              }
            }
            val start_time = json.getOrDefault("start_time","1970-01-01 08:00:00")
            val end_time = json.getOrDefault("end_time","1970-01-01 08:00:00")
            val view_count = json.getOrDefault("view_count","0")
            val view_count_robot = json.getOrDefault("view_count_robot","0")
            val max_living_count = json.getOrDefault("max_living_count","0")
            val comment_count = json.getOrDefault("comment_count","0")
            val comment_count_robot = json.getOrDefault("comment_count_robot","0")
            val comment_peoples = json.getOrDefault("comment_peoples","0")
            val comment_peoples_robot = json.getOrDefault("comment_peoples_robot","0")
            val gift_income = json.getOrDefault("gift_income","0")
            val live_times = if(start_time == "1970-01-01 08:00:00"|| end_time == "1970-01-01 08:00:00") 0 else (sdf.parse(end_time).getTime - sdf.parse(start_time).getTime)/1000
            (1,live_times,view_count,view_count_robot,max_living_count,comment_count,comment_count_robot,comment_peoples,comment_peoples_robot,gift_income)
          })
            if (lcsCountResultList.nonEmpty){
              val lcsCountResult = lcsCountResultList.reduce((v1,v2) => {
                  (
                    v1._1 + v2._1,
                    v1._2 + v2._2,
                    v1._3.toInt + v2._3.toInt + "",
                    v1._4.toInt + v2._4.toInt + "",
                    math.max(v1._5.toInt , v2._5.toInt) + "",
                    v1._6.toInt + v2._6.toInt + "",
                    v1._7.toInt + v2._7.toInt + "",
                    v1._8.toInt + v2._8.toInt + "",
                    v1._9.toInt + v2._9.toInt + "",
                    (v1._10.toDouble + v2._10.toDouble).formatted("%.2f") + ""
                  )
                })
              val jsonObj2 = new JSONObject
              jsonObj2.put("notice_num", lcsCountResult._1 + "")
              jsonObj2.put("live_times", lcsCountResult._2 + "")
              jsonObj2.put("view_count", lcsCountResult._3)
              jsonObj2.put("view_count_robot", lcsCountResult._4)
              jsonObj2.put("max_living_count", lcsCountResult._5)
              jsonObj2.put("comment_count", lcsCountResult._6)
              jsonObj2.put("comment_count_robot", lcsCountResult._7)
              jsonObj2.put("comment_peoples", lcsCountResult._8)
              jsonObj2.put("comment_peoples_robot", lcsCountResult._9)
              jsonObj2.put("gift_income", lcsCountResult._10)
              jedis.hset("lcs:live:visit:count:circle", lcs_id , jsonObj2.toJSONString())
            }

          LcsLiveVisitCount(live_id, circle_id, lcs_id, lcs_name, start_time, end_time, live_title, live_status, view_count+"", view_count_robot+"", first_view_count+"",
            max_living_count+"", new_follow_count+"", new_follow_count_robot+"", comment_count+"", comment_count_robot+"", comment_peoples+"", comment_peoples_robot+"",
            gift_count+"", gift_count_robot+"", share_count+"", share_count_robot+"", old_user_staying_average+"", old_user_staying_max+"", old_user_staying_min+"",
            new_user_staying_average+"", new_user_staying_max+"", new_user_staying_min+"")
        })

//        htable.close()
//        conn.close()
        jedis.close()
//        jedis2.close()
        result
      })
      .write
      .mode(Overwrite)
      .format("Hive")
      .saveAsTable("ads.ads_lcs_live_visit_count")

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
                                new_user_staying_min: String)

}
