package cn.yintech.online

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, SQLException, Statement}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import cn.yintech.esUtil.ESConfig
import cn.yintech.hbaseUtil.HbaseUtils.{getHbaseConf, getRow, setRow}
import cn.yintech.redisUtil.{RedisClient, RedisClientNew}
import net.minidev.json.parser.JSONParser
import net.minidev.json.{JSONArray, JSONObject}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
 *  直播间统计实时部分
 */
object LiveVisitOnline {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("LiveVisitOnline") //.setMaster("local[100]")
      .set("spark.default.parallelism", "100")
    val ssc = new StreamingContext(conf, Seconds(60))
    //    ssc.checkpoint("./LiveVisitOnline")
    //    ssc.checkpoint("hdfs:///user/licaishi/LiveVisitOnline")

    val lineDStream = ssc.receiverStream(new CustomReceiverFromMysql())

    lineDStream.map(line => {
      val dataMap = jsonParse(line)
      (
        dataMap.getOrElse("id", ""),
        dataMap.getOrElse("circle_id", ""),
        dataMap.getOrElse("u_type", ""),
        dataMap.getOrElse("uid", ""),
        dataMap.getOrElse("title", ""),
        dataMap.getOrElse("start_time", "").substring(0,19),
        dataMap.getOrElse("end_time", "").substring(0,19),
        dataMap.getOrElse("live_status", "")

      )

    }).filter(v => v._8 == "0" || v._8 == "1").repartition(30)
      .foreachRDD(rdd => {
        println("rdd part:" + rdd.partitioner,rdd.id,rdd.name)
        rdd.foreachPartition(r => {
          // hbase连接初始化
          val conn = ConnectionFactory.createConnection(getHbaseConf)
          val table = TableName.valueOf("user_live_visit_lcs")
          val htable = conn.getTable(table)
          val table2 = TableName.valueOf("lcs_live_visit_new_user")
          val htable2 = conn.getTable(table2)
          // redis连接初始化
          val jedis = RedisClient.pool.getResource
          val jedis2 = RedisClientNew.pool.getResource //新redis
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          //创建mysql连接
          val connection = DriverManager.getConnection("jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "syl_w", "naAm7kmYgaG7SrkO1mAT")
          //        val connection = DriverManager.getConnection("jdbc:mysql://localhost/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "root", "root")
          val sql1 =
            """
              |UPDATE lcs_planner_live_info set
              |circle_id = ?,
              |notice_id = ?,
              |p_uid = ?,
              |live_title = ?,
              |view_count = ?,
              |first_view_count = ?,
              |max_living_count = ?,
              |new_follow_count = ?,
              |comment_count = ?,
              |comment_peoples = ?,
              |gift_count = ?,
              |share_count = ?,
              |old_user_staying_average = ?,
              |old_user_staying_max = ?,
              |old_user_staying_min = ?,
              |new_user_staying_average = ?,
              |new_user_staying_max = ?,
              |new_user_staying_min = ?,
              |planner_comment_count = ?,
              |planner_reply_count = ?,
              |view_count_robot = ?,
              |new_follow_count_robot = ?,
              |comment_count_robot = ?,
              |comment_peoples_robot = ?,
              |gift_count_robot = ?,
              |share_count_robot = ?
              |
              | WHERE notice_id = ? ;
            """.stripMargin
          val sql2 = """
                       |insert into lcs_planner_live_info (
                       |circle_id,
                       |notice_id,
                       |p_uid,
                       |live_title,
                       |view_count,
                       |first_view_count,
                       |max_living_count,
                       |new_follow_count,
                       |comment_count,
                       |comment_peoples,
                       |gift_count,
                       |share_count,
                       |old_user_staying_average,
                       |old_user_staying_max,
                       |old_user_staying_min,
                       |new_user_staying_average,
                       |new_user_staying_max,
                       |new_user_staying_min,
                       |planner_comment_count,
                       |planner_reply_count,
                       |view_count_robot,
                       |new_follow_count_robot,
                       |comment_count_robot,
                       |comment_peoples_robot,
                       |gift_count_robot,
                       |share_count_robot,
                       |c_time,
                       |u_time
                       |) SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                       |FROM
                       |	DUAL
                       |WHERE
                       |	NOT EXISTS (
                       |		SELECT
                       |			notice_id
                       |		FROM
                       |			lcs_planner_live_info
                       |		WHERE
                       |			notice_id = ?
                       |	);
                       |""".stripMargin

          try {
            r.foreach(v => {
              val ps1 = connection.prepareStatement(sql1)
              val ps2 = connection.prepareStatement(sql2)
              import scala.collection.JavaConversions._
              // redis上一次数据
              val oldValue = jedis.hget("lcs:live:visit:count", v._1)
              var old_max_living_count = 0 // 前次最高在线人数
              var old_first_view_count = 0 // 前次首次观看人数
              var old_live_status = -1

              var pre_new_user_staying_average = 0.0
              var pre_new_user_staying_max = 0
              var pre_new_user_staying_min = 0

              if (oldValue != null) {
                old_max_living_count = jsonParse(oldValue).getOrDefault("max_living_count", "0").toInt
                old_first_view_count = jsonParse(oldValue).getOrDefault("first_view_count", "0").toInt
                old_live_status = jsonParse(oldValue).getOrDefault("live_status", "-1").toInt
                pre_new_user_staying_average = jsonParse(oldValue).getOrDefault("new_user_staying_average", "0").toDouble
                pre_new_user_staying_max = jsonParse(oldValue).getOrDefault("new_user_staying_max", "0").toInt
                pre_new_user_staying_min = jsonParse(oldValue).getOrDefault("new_user_staying_min", "0").toInt
              }
              if (old_live_status != 0) {
                var end = v._7
                if (v._8 == "1")
                  end = sdf.format(new Date())
                // 线上lcs_comment_master评论表
                val comment = readComment(v._2, v._6, end) // u_type,uid,discussion_type,is_robot,reply_id
                val commentGroup = comment.filter(_.head == "1").map(v => (v.head, v(1), v(2), v(3))).groupBy(_._3)
                val commentGroupLcs = comment.filter(v => v.head == "2" && v(2) == "0" ) //理财师发言记录 u_type = 2 AND discussion_type = 0
                // 老师发言数
                val planner_comment_count = commentGroupLcs.count(_ (4) == "0")
                // 老师回复数
                val planner_reply_count = commentGroupLcs.count(_ (4) != "0")

                // ES数据用户观看记录
                val onlineFromEs = ESConfig.searchOnlineAgg(v._2, v._6, end)
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
                val onlineFromEsCurrent = ESConfig.searchOnlineAgg(v._2, "now-60s", "now")


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
                val oldUser = onlineFromEsList.filter(v =>  !allNewUsers.contains(v._3) || v._3 == "" ) // 用户不在新观众列表或者uid为空
                // 新用户
                val newUser = onlineFromEsList.filter(v => allNewUsers.contains(v._3) &&  v._3.length > 0 )// 用户在新观众列表且uid不为空
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
                val max_living_count = math.max(onlineFromEsCurrent.size, old_max_living_count)

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
                  math.min((if (oldUser.isEmpty) 0 else oldUser.map(_._4).sum * 1.0 / oldUser.size),(sdf.parse(end).getTime-sdf.parse(v._6).getTime)/1000 )
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
                jsonObj.put("planner_comment_count", planner_comment_count + "")
                jsonObj.put("planner_reply_count", planner_reply_count + "")

                // 直播场次维度结果存入redis hash
                jedis.hset("lcs:live:visit:count", live_id, jsonObj.toJSONString())
                jedis2.hset("lcs:live:visit:count", live_id, jsonObj.toJSONString()) // 新redis
                // 每场次uid存入set集合
                val deviceidUseridMap = Map(onlineFromEsList.map(v => (v._5,v._3)).sortBy(_._2.length):_*)
                jedis2.del(s"lcs:live:visit:uid:$live_id") // 清空set集合
                deviceidUseridMap
                  .foreach( v  => {
                  if (v._2.length > 0){
//                    jedis.sadd(s"lcs:live:visit:uid:$live_id",v._2)
//                    jedis.expire(s"lcs:live:visit:uid:$live_id", 2*24*60*60 )
                    // 新redis
                    jedis2.sadd(s"lcs:live:visit:uid:$live_id",v._2)
                    jedis2.expire(s"lcs:live:visit:uid:$live_id", 2*24*60*60 )
                  } else if (v._1.length > 0){
//                    jedis.sadd(s"lcs:live:visit:uid:$live_id",v._1)
//                    jedis.expire(s"lcs:live:visit:uid:$live_id", 2*24*60*60 )
                    // 新redis
                    jedis2.sadd(s"lcs:live:visit:uid:$live_id",v._1)
                    jedis2.expire(s"lcs:live:visit:uid:$live_id", 2*24*60*60 )
                  }
                })

                // 理财师维度结果 存入redis hash
                val lcsCountResultList = readNoticeId(lcs_id).map( v => {
                  var jsonStr = jedis.hget("lcs:live:visit:count",v)
                  if (jsonStr == null || jsonStr == "" ) jsonStr = "{}"
                  val json = jsonParse(jsonStr)
                  val start_time = json.getOrElse("start_time","1970-01-01 08:00:00")
                  val end_time = json.getOrElse("end_time","1970-01-01 08:00:00")
                  val view_count = json.getOrElse("view_count","0")
                  val view_count_robot = json.getOrElse("view_count_robot","0")
                  val max_living_count = json.getOrElse("max_living_count","0")
                  val comment_count = json.getOrElse("comment_count","0")
                  val comment_count_robot = json.getOrElse("comment_count_robot","0")
                  val comment_peoples = json.getOrElse("comment_peoples","0")
                  val comment_peoples_robot = json.getOrElse("comment_peoples_robot","0")
                  val gift_income = json.getOrElse("gift_income","0.0")
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

                // 直播场次维度结果存入mysql
                val nowTime = sdf.format(new Date())
                ps1.setInt(1,circle_id.toInt)
                ps1.setInt(2,live_id.toInt)
                ps1.setLong(3,lcs_id.toLong)
                ps1.setString(4,live_title)
                ps1.setInt(5,view_count.toInt)
                ps1.setInt(6,first_view_count.toInt)
                ps1.setLong(7,max_living_count.toInt)
                ps1.setInt(8,new_follow_count.toInt)
                ps1.setInt(9,comment_count.toInt)
                ps1.setLong(10,comment_peoples.toInt)
                ps1.setInt(11,gift_count.toInt)
                ps1.setInt(12,share_count.toInt)
                ps1.setDouble(13,old_user_staying_average.toDouble)
                ps1.setDouble(14,old_user_staying_max.toDouble)
                ps1.setDouble(15,old_user_staying_min.toDouble)
                ps1.setDouble(16,new_user_staying_average.toDouble)
                ps1.setDouble(17,new_user_staying_max.toDouble)
                ps1.setDouble(18,new_user_staying_min.toDouble)
                ps1.setInt(19,planner_comment_count.toInt)
                ps1.setInt(20,planner_reply_count.toInt)
                ps1.setInt(21,view_count_robot.toInt)
                ps1.setInt(22,new_follow_count_robot.toInt)
                ps1.setInt(23,comment_count_robot.toInt)
                ps1.setInt(24,comment_peoples_robot.toInt)
                ps1.setInt(25,gift_count_robot.toInt)
                ps1.setInt(26,share_count_robot.toInt)

                ps1.setInt(27,live_id.toInt)
                // ------------------------------------------------------
                ps2.setInt(1,circle_id.toInt)
                ps2.setInt(2,live_id.toInt)
                ps2.setLong(3,lcs_id.toLong)
                ps2.setString(4,live_title)
                ps2.setInt(5,view_count.toInt)
                ps2.setInt(6,first_view_count.toInt)
                ps2.setLong(7,max_living_count.toInt)
                ps2.setInt(8,new_follow_count.toInt)
                ps2.setInt(9,comment_count.toInt)
                ps2.setLong(10,comment_peoples.toInt)
                ps2.setInt(11,gift_count.toInt)
                ps2.setInt(12,share_count.toInt)
                ps2.setDouble(13,old_user_staying_average.toDouble)
                ps2.setDouble(14,old_user_staying_max.toDouble)
                ps2.setDouble(15,old_user_staying_min.toDouble)
                ps2.setDouble(16,new_user_staying_average.toDouble)
                ps2.setDouble(17,new_user_staying_max.toDouble)
                ps2.setDouble(18,new_user_staying_min.toDouble)
                ps2.setInt(19,planner_comment_count.toInt)
                ps2.setInt(20,planner_reply_count.toInt)
                ps2.setInt(21,view_count_robot.toInt)
                ps2.setInt(22,new_follow_count_robot.toInt)
                ps2.setInt(23,comment_count_robot.toInt)
                ps2.setInt(24,comment_peoples_robot.toInt)
                ps2.setInt(25,gift_count_robot.toInt)
                ps2.setInt(26,share_count_robot.toInt)
                ps2.setString(27,nowTime)
                ps2.setString(28,nowTime)

                ps2.setInt(29,live_id.toInt)


//                ps1.addBatch()
//                ps2.addBatch()
                ps1.executeUpdate()
                ps1.close()
                ps2.executeUpdate()
                ps2.close()
              }

            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            conn.close()
            jedis.close()
            jedis2.close()

            connection.close()
          }

        })
      })


    //启动
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true)

  }

  /**
   * 根据时间段读取用户评论表
   * @param extraId
   * @param startTime
   * @param endTime
   * @return
   */
  def readComment(extraId: String, startTime: String, endTime: String): List[Seq[String]] = {
    var conn: Connection = null
    var stmt: Statement = null
    var result: List[Seq[String]] = List()

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      //      conn = DriverManager.getConnection("jdbc:mysql://rm-2zebtm824um01072vrw.mysql.rds.aliyuncs.com/licaishi_comment?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "lcs_comment_r", "@Licaishi201707")
      conn = DriverManager.getConnection("jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi_comment?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull", "lcs_comment_r", "3c05068bb4a5cd6")
      stmt = conn.createStatement
      val sql = s"SELECT u_type,uid,discussion_type,is_robot,reply_id from  lcs_comment_master WHERE c_time >= '$startTime' and c_time <= '$endTime' and relation_id = '$extraId' "
      val rs: ResultSet = stmt.executeQuery(sql)

      // json数组
      // 遍历ResultSet中的每条数据
      while (rs.next()) {
        val u_type = rs.getInt("u_type")
        val uid = rs.getLong("uid")
        val discussion_type = rs.getInt("discussion_type")
        val is_robot = rs.getInt("is_robot")
        val reply_id = rs.getInt("reply_id")
        result = result.::(Seq(u_type.toString, uid.toString, discussion_type.toString, is_robot.toString, reply_id.toString))
      }
      // 完成后关闭
      rs.close()
      stmt.close()
      conn.close()
    } catch {
      case t: Exception => t.printStackTrace() // TODO: handle error
    } finally {
      try if (stmt != null) stmt.close()
      catch {
        case se1: SQLException =>
          se1.printStackTrace()
      }
      try if (conn != null) conn.close()
      catch {
        case se2: SQLException =>
          se2.printStackTrace()
      }
    }
    result
  }

  /**
   * 根据时间段读取lcs_planner_incomen表计算礼物收益
   * @param lcsId
   * @param startTime
   * @param endTime
   * @return
   */
  def readGiftIncome(lcsId: String , startTime: String, endTime: String): Double = {
    var conn: Connection = null
    var stmt: Statement = null
    var result: Double = 0.0

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
//                conn = DriverManager.getConnection("jdbc:mysql://rm-2zebtm824um01072v5o.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "lcs_spider_r", "qE1$eB1*mF3}")
      conn = DriverManager.getConnection("jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull", "syl_w", "naAm7kmYgaG7SrkO1mAT")
      stmt = conn.createStatement
      val sql = s"SELECT sum(divide_money) income from lcs_planner_income WHERE p_uid = $lcsId and `status` = 0 and c_time BETWEEN '$startTime' and '$endTime' "
      val rs: ResultSet = stmt.executeQuery(sql)

      // json数组
      // 遍历ResultSet中的每条数据
      while (rs.next()) {
        result = rs.getDouble("income")
      }
      // 完成后关闭
      rs.close()
      stmt.close()
      conn.close()
    } catch {
      case t: Exception => t.printStackTrace() // TODO: handle error
    } finally {
      try if (stmt != null) stmt.close()
      catch {
        case se1: SQLException =>
          se1.printStackTrace()
      }
      try if (conn != null) conn.close()
      catch {
        case se2: SQLException =>
          se2.printStackTrace()
      }
    }
    result
  }

  /**
   * 根据lcsId查询lcs_circle_notice表返回直播场次id列表
   * @param lcsId
   * @return
   */
  def readNoticeId(lcsId: String): List[String] = {
    var conn: Connection = null
    var stmt: Statement = null
    var result: List[String] = List()

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
//      conn = DriverManager.getConnection("jdbc:mysql://rm-2zebtm824um01072v5o.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "lcs_spider_r", "qE1$eB1*mF3}")
            conn = DriverManager.getConnection("jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull", "syl_w", "naAm7kmYgaG7SrkO1mAT")
      stmt = conn.createStatement
      val sql = s"SELECT id from  lcs_circle_notice WHERE uid = $lcsId and live_status in (0,1) AND (`u_type`=2) AND (`type`=4) AND (`audit`=1) AND (`status`=0)  "
      val rs: ResultSet = stmt.executeQuery(sql)

      // json数组
      // 遍历ResultSet中的每条数据
      while (rs.next()) {
        result = result.::(rs.getLong("id") + "")
      }
      // 完成后关闭
      rs.close()
      stmt.close()
      conn.close()
    } catch {
      case t: Exception => t.printStackTrace() // TODO: handle error
    } finally {
      try if (stmt != null) stmt.close()
      catch {
        case se1: SQLException =>
          se1.printStackTrace()
      }
      try if (conn != null) conn.close()
      catch {
        case se2: SQLException =>
          se2.printStackTrace()
      }
    }
    result
  }
  /**
   * json字符串解析
   *
   * @param value
   * @return
   */
  def jsonParse(value: String): Map[String, String] = {
    var map = Map[String, String]()
    val jsonParser = new JSONParser()
    try {
      val outJsonObj: JSONObject = jsonParser.parse(value).asInstanceOf[JSONObject]
      val outJsonKey = outJsonObj.keySet()
      val outIter = outJsonKey.iterator

      while (outIter.hasNext) {
        val outKey = outIter.next()
        val outValue = if (outJsonObj.get(outKey) != null) outJsonObj.get(outKey).toString else "null"
        map += (outKey -> outValue)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        println("Exception:" + value)
      }
    }
    map
  }

  class CustomReceiverFromMysql() extends Receiver[String](StorageLevel.DISK_ONLY) {
    override def onStart(): Unit = {
      println(new Date() + ":CustomReceiverFromMysql,read mysql")

      while (!isStopped()) {
        var conn: Connection = null
        var stmt: Statement = null
        try {
          Class.forName("com.mysql.jdbc.Driver").newInstance()
          //          conn = DriverManager.getConnection("jdbc:mysql://rm-2zebtm824um01072v5o.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "lcs_spider_r", "qE1$eB1*mF3}")
          conn = DriverManager.getConnection("jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull", "syl_w", "naAm7kmYgaG7SrkO1mAT")
          stmt = conn.createStatement
          val sdf = new SimpleDateFormat("yyyy-MM-dd")
          val today = sdf.format(new Date)
          val sql = s"SELECT * from  lcs_circle_notice WHERE end_time > '$today' and live_status in (0,1) "
          val rs: ResultSet = stmt.executeQuery(sql)

          // json数组
          val result = new util.ArrayList[String]()

          // 获取列数
          val metaData: ResultSetMetaData = rs.getMetaData()
          val columnCount = metaData.getColumnCount()
          // 遍历ResultSet中的每条数据
          while (rs.next()) {
            val jsonObj = new JSONObject();
            // 遍历每一列
            for (i <- 1 to columnCount) {
              val columnName = metaData.getColumnLabel(i)
              val value = rs.getString(columnName)
              jsonObj.put(columnName, value)
            }
            result.add(jsonObj.toJSONString)
          }
          if (!result.isEmpty)
            store(result.iterator())
          // 完成后关闭
          rs.close()
          stmt.close()
          conn.close()
          println(new Date() + ":size================== " + result.size())
          Thread.sleep(60000)
        } catch {
          case t: Exception => t.printStackTrace() // TODO: handle error
        } finally {
          try if (stmt != null) stmt.close()
          catch {
            case se1: SQLException =>
              se1.printStackTrace()
          }
          try if (conn != null) conn.close()
          catch {
            case se2: SQLException =>
              se2.printStackTrace()
          }
        }

      }
    }

    override def onStop(): Unit = {
      println(new Date() + ":CustomReceiverFromMysql,finish mysql")
    }
  }

}
