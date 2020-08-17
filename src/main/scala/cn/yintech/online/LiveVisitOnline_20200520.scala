package cn.yintech.online

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, SQLException, Statement}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import cn.yintech.esUtil.ESConfig
import cn.yintech.hbaseUtil.HbaseUtils.{getHbaseConf, getRow, setRow}
import cn.yintech.redisUtil.RedisClient
import net.minidev.json.parser.JSONParser
import net.minidev.json.{JSONArray, JSONObject}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

object LiveVisitOnline_20200520 {

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

    }).filter(v => v._8 == "0" || v._8 == "1").repartition(20)
      .foreachRDD(rdd => {
        rdd.foreachPartition(r => {
          // hbase连接初始化
          val conn = ConnectionFactory.createConnection(getHbaseConf)
          val table = TableName.valueOf("user_live_visit_lcs")
          val htable = conn.getTable(table)
          val table2 = TableName.valueOf("lcs_live_visit_new_user")
          val htable2 = conn.getTable(table2)
          // redis连接初始化
          val jedis = RedisClient.pool.getResource
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          try {
            r.foreach(v => {
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
                // 评论表
                val comment = readComment(v._2, v._6, end) // u_type,uid,discussion_type,is_robot
                val commentGroup = comment.map(v => (v.head, v(1), v(2), v(3))).groupBy(_._3)
                // ES数据用户观看记录
                val onlineFromEs = ESConfig.searchOnlineAgg(v._2, v._6, end)
                val onlineFromEsList = onlineFromEs.map(v => {
                  val json = jsonParse(v)
                  (
                    json.getOrElse("extra_id", ""),
                    json.getOrElse("type", ""),
                    json.getOrElse("uid", ""),
                    json.getOrElse("points", "").toInt * 30
                  )
                }).filter(v => v._2 == "圈子视频直播" && v._3.length > 0)
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
                val oldUser = onlineFromEsList.filter(v => !allNewUsers.contains(v._3))
                // 新用户
                val newUser = onlineFromEsList.filter(v => allNewUsers.contains(v._3))
                // 结果字段
                val live_id = v._1
                val circle_id = v._2
                val lcs_id = v._4
                val lcs_name = "#"
                val start_time = v._6
                val end_time = v._7
                val live_title = v._5
                val live_status = v._8
                val view_count = onlineFromEsList.size
                val view_count_robot = comment.count(_ (3) == "1")
                val first_view_count = newUser.size
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

                jedis.hset("lcs:live:visit:count", v._1, jsonObj.toJSONString())
                println(jsonObj.toJSONString())
              }

            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            conn.close()
            jedis.close()
          }

        })
      })


    //启动
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true)

  }

  def readComment(extraId: String, startTime: String, endTime: String): List[Seq[String]] = {
    var conn: Connection = null
    var stmt: Statement = null
    var result: List[Seq[String]] = List()

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      //      conn = DriverManager.getConnection("jdbc:mysql://rm-2zebtm824um01072vrw.mysql.rds.aliyuncs.com/licaishi_comment?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "lcs_comment_r", "@Licaishi201707")
      conn = DriverManager.getConnection("jdbc:mysql://rm-2zebtm824um01072v.mysql.rds.aliyuncs.com/licaishi_comment?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "lcs_comment_r", "@Licaishi201707")
      stmt = conn.createStatement
      val sql = s"SELECT u_type,uid,discussion_type,is_robot from  lcs_comment_master WHERE c_time >= '$startTime' and c_time <= '$endTime' and relation_id = '$extraId' and u_type = 1 "
      val rs: ResultSet = stmt.executeQuery(sql)

      // json数组
      // 遍历ResultSet中的每条数据
      while (rs.next()) {
        val u_type = rs.getInt("u_type")
        val uid = rs.getLong("uid")
        val discussion_type = rs.getInt("discussion_type")
        val is_robot = rs.getInt("is_robot")
        result = result.::(Seq(u_type.toString, uid.toString, discussion_type.toString, is_robot.toString))
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
          conn = DriverManager.getConnection("jdbc:mysql://rm-2zebtm824um01072v.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "lcs_spider_r", "qE1$eB1*mF3}")
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
