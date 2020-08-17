import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement}
import java.text.{ParseException, SimpleDateFormat}
import java.util
import java.util.{Calendar, Date, Iterator, Set, TimeZone}

import cn.yintech.esUtil.ESConfig
import cn.yintech.online.LiveVisitOnline.{jsonParse, readGiftIncome, readNoticeId}
import cn.yintech.redisUtil.RedisClient
import net.minidev.json.{JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import cn.yintech.online.LiveVisitOnline
import net.minidev.json.parser.JSONParser
import cn.yintech.online.LiveVisitOnline.jsonParse
import cn.yintech.eventLog.SparkReadEsRealTimeCount.esSearch
import cn.yintech.eventLog.SparkReadEsPro_yii2_elk_success.{splitDayMinute2,esSearchPro_yii2_elk_success1}

object Test {
  def main(args: Array[String]): Unit = {
//    val jedis = RedisClient.pool.getResource
//val json = jsonParse("{}")
    println(esSearchPro_yii2_elk_success1("2020-07-21 00:00:00","2020-07-21 03:00:00").size)
//    println(esSearch("61193","2020-01-02 11:33:02","2020-01-02 12:37:16"))
//    val jsonStr = jedis.hget("lcs:live:visit:count","49262")
//    val json = jsonParse(jsonStr)
//    println(json)
//    val gift_income = json.getOrElse("gift_income","0")
//
//    println(gift_income.toDouble)
//    jedis.close()

//    // 理财师维度结果
//
//    val ids = Array("6020458946","6326610413","3158802790","6304215484","1603147504","5639072717")
//
//
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
////    readLcsId().foreach( lcs_id => {
//    ids.foreach( lcs_id => {
//      println(lcs_id + "...")
//      var nums = 0
//      val lcsCountResultList = readNoticeId(lcs_id).map( v => {
//
//        nums += 1
//        val jsonStr = jedis.hget("lcs:live:visit:count",v)
//        val json = jsonParse(jsonStr)
//        val start_time = json.getOrElse("start_time","1970-01-01 08:00:00")
//        val end_time = json.getOrElse("end_time","1970-01-01 08:00:00")
//        val view_count = json.getOrElse("view_count","0")
//        val view_count_robot = json.getOrElse("view_count_robot","0")
//        val max_living_count = json.getOrElse("max_living_count","0")
//        val comment_count = json.getOrElse("comment_count","0")
//        val comment_count_robot = json.getOrElse("comment_count_robot","0")
//        val comment_peoples = json.getOrElse("comment_peoples","0")
//        val comment_peoples_robot = json.getOrElse("comment_peoples_robot","0")
//        val gift_income = json.getOrElse("gift_income","0.0")
//        val live_times = (sdf.parse(end_time).getTime - sdf.parse(start_time).getTime)/1000
//        (1,live_times,view_count,view_count_robot,max_living_count,comment_count,comment_count_robot,comment_peoples,comment_peoples_robot,gift_income)
//      })
//
//      if (lcsCountResultList.nonEmpty){
//        val lcsCountResult = lcsCountResultList.reduce((v1,v2) => {
//
//          (
//            v1._1 + v2._1,
//            v1._2 + v2._2,
//            v1._3.toInt + v2._3.toInt + "",
//            v1._4.toInt + v2._4.toInt + "",
//            math.max(v1._5.toInt , v2._5.toInt) + "",
//            v1._6.toInt + v2._6.toInt + "",
//            v1._7.toInt + v2._7.toInt + "",
//            v1._8.toInt + v2._8.toInt + "",
//            v1._9.toInt + v2._9.toInt + "",
//
//            (v1._10.toDouble + v2._10.toDouble).formatted("%.2f") + ""
//          )
//        })
//        val jsonObj2 = new JSONObject
//        jsonObj2.put("notice_num", lcsCountResult._1 + "")
//        jsonObj2.put("live_times", lcsCountResult._2 + "")
//        jsonObj2.put("view_count", lcsCountResult._3)
//        jsonObj2.put("view_count_robot", lcsCountResult._4)
//        jsonObj2.put("max_living_count", lcsCountResult._5)
//        jsonObj2.put("comment_count", lcsCountResult._6)
//        jsonObj2.put("comment_count_robot", lcsCountResult._7)
//        jsonObj2.put("comment_peoples", lcsCountResult._8)
//        jsonObj2.put("comment_peoples_robot", lcsCountResult._9)
//        jsonObj2.put("gift_income", lcsCountResult._10)
//
////        jedis.hset("lcs:live:visit:count:circle", lcs_id , jsonObj2.toJSONString())
//        println(lcs_id + " end , nums :" + nums)
//      }
//
//      println()
//    })
//    jedis.close()

    val sql =
      """
        |insert overwrite table dwd.dwd_base_event_1d
        |PARTITION (dt='2020-08-05')
        |select
        |get_json_object(line,'$._track_id')  tmp_track_id,
        |get_json_object(line,'$.time')  time,
        |get_json_object(line,'$.type')  type,
        |get_json_object(line,'$.distinct_id')  distinct_id,
        |get_json_object(line,'$.lib')  lib,
        |get_json_object(line,'$.event')  event,
        |get_json_object(line,'$.properties')  properties,
        |get_json_object(line,'$._flush_time')  tmp_flush_time,
        |get_json_object(line,'$.map_id')  map_id,
        |get_json_object(line,'$.user_id')  user_id,
        |get_json_object(line,'$.recv_time')  recv_time,
        |get_json_object(line,'$.extractor') extractor,
        |get_json_object(line,'$.project_id') project_id,
        |get_json_object(line,'$.project')  project,
        |get_json_object(line,'$.ver')  ver
        |from (select * from ods.ods_event_log_1d where dt='2020-08-05' and line is not null) t
        |
        |""".stripMargin

//    import scala.collection.JavaConversions
//
//    JavaConversions.mapAsScalaMap(jedis.hgetAll("lcs:live:visit:count"))
//        .map( v => {
//          val jsonParser = new JSONParser()
//          val outJsonObj: JSONObject = jsonParser.parse(v._2).asInstanceOf[JSONObject]
//          val jsonStr = outJsonObj.appendField("gift_income","0.0").toJSONString()
//          jedis.hset("lcs:live:visit:count", v._1, jsonStr)
//        })
//    import scala.collection.JavaConversions._
//    val onlineFromEs = ESConfig.searchOnlineAgg("61260", "2020-05-20 14:09:41", "2020-05-20 15:34:29")
//    val onlineFromEsList = onlineFromEs
//      .map(v => {
//      val json = jsonParse(v)
//      (
//        json.getOrElse("extra_id", ""),
//        json.getOrElse("type", ""),
//        json.getOrElse("uid", ""),
//        json.getOrElse("points", "").toInt * 30,
//        json.getOrElse("device_id", "")
//      )
//    }).filter(v => v._2 == "圈子视频直播" && v._3.length > 0  && v._5.length > 0)
//
//    println(onlineFromEsList.map(_._5).distinct.size)



//    val preJson = jedis.hget("lcs:live:visit:count","47942")
//    println(preJson)
//    val jsonParser = new JSONParser()
//    val outJsonObj: JSONObject = jsonParser.parse(preJson).asInstanceOf[JSONObject]
//    val jsonStr = outJsonObj.appendField("gift_income","0.0").toJSONString()
//    println(jsonStr)
//    jedis.hset("lcs:live:visit:count", "47942", jsonStr)



//    println(readGiftIncome("1160176332","2020-05-19","2020-05-20"))

//    val aaa = Array("2020-22-08 ")
//    require(!(aaa == null || aaa.length == 0 || aaa(0) == ""), "Required 'dt' arg")
//    val pattern = new Regex("^[1-9]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])$") // 日期格式
//    val pattern2 = new Regex("^[1-9]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\\s+(20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d$") // 日期时间格式
//    val dateSome = pattern findFirstIn aaa(0)
//    val dateSome2 = pattern2 findFirstIn aaa(0)
//    require(dateSome.isDefined || dateSome2.isDefined , s"Required PARTITION aaa like 'yyyy-MM-dd' OR like 'yyyy-MM-dd HH:mm:ss' but find ${aaa(0)}")
//    val dt = if (dateSome.isDefined) dateSome.get else dateSome2.get // 实际使用yyyy-mm-dd格式日期
//    println("update dt : " + dt)
    //    val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2020-03-26 17:06:18")
    //    val time2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2020-03-26 17:06:01")
    //    println(""!="")
    //    println(time2.getTime - time1.getTime )
    //    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(1582495797808L))
    //    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    //    val cal = Calendar.getInstance()
    //    cal.setTime(sdf.parse("2020-04-09"))
    //    cal.add(Calendar.DAY_OF_YEAR,1)
    //    val t = sdf.format(cal.getTime)
    //    println(t)

    //    println(new Date().getTime)
    //      println(math.ceil(7.2).toInt)

    //    val jsonParser = new JSONParser()
    //    jsonParser.ensuring(true)
    //    val outJsonObj = jsonParser.parse("{sdf}")
    //    println(outJsonObj.isInstanceOf[JSONObject])

    //    val jsonObj = JSON.parse("{xds}")
    //    println(jsonObj == null)

    //      .asInstanceOf[JSONObject]
    //    val outJsonKey = outJsonObj.keySet()
    //    val outIter = outJsonKey.iterator
    //    println(outIter.hasNext)

    //    val pattern = new Regex("/[^,:{}\\[\\]0-9.\\-+Eaeflnr-u \\n\\r\\t]/")
    //    val dateSome = pattern findFirstIn "{fsdf:32}"
    //    println(dateSome.isDefined)
    //    val ite = Iterator("d","g")
    //    val set = ite.toSet
    //    println(set)

    //    val jsonO = new JSONObject
    //    jsonO.put("name","32")
    //    jsonO.put("age",new Integer(10))
    //    jsonO.put("time", "32")
    //    println(jsonO.toJSONString)


    //    val createTime = "0"
//    val sdf = new SimpleDateFormat("HH:mm:ss")
//    val minutes = (0 until 720).flatMap( i => {
//      if (i == 1439)
//        Array((sdf.format(-28800000+i*1000*60),"23:59:30"),
//          ("23:59:30","24:00:00"))
//      else
//        Array((sdf.format(-28800000+i*1000*60),sdf.format(-28800000+(i+1)*1000*60-30000)),
//          (sdf.format(-28800000+(i+1)*1000*60-30000),sdf.format(-28800000+(i+1)*1000*60)))
//    })
//    val minutes2 = (720 until 1440).flatMap( i => {
//      if (i == 1439)
//        Array((sdf.format(-28800000+i*1000*60),"23:59:30"),
//          ("23:59:30","24:00:00"))
//      else
//        Array((sdf.format(-28800000+i*1000*60),sdf.format(-28800000+(i+1)*1000*60-30000)),
//          (sdf.format(-28800000+(i+1)*1000*60-30000),sdf.format(-28800000+(i+1)*1000*60)))
//    })

    //    try {
    //      println(sdf.parse(createTime).getTime)
    //    } catch  {
    //      case ex : ParseException => {
    //          ex.printStackTrace()
    //      }
    //    }

    //    def msTillTomorrow = {
    //      val now = new Date()
    //      val tomorrow = new Date(now.getYear, now.getMonth, now.getDate + 1)
    //      tomorrow.getTime - now.getTime
    //    }
    //
    //    println(msTillTomorrow)

    //    print(Map("extra_id"-> 1,"type"->2,"online"->"3","average"->"aa").toString())
    //    val  sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    //    sdf.setTimeZone(TimeZone.getTimeZone("UTC-0"))
    //    println(sdf.format(new Date())+"Z")

    //    val string = "你好"
    //    val str = new String(string.getBytes(),0,string.getBytes().length,"UTF-8")
    //    println(string.getBytes("GBK"))
    //    println(str)
    // 2020-04-02T17:51:04+08:00
    //val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    ////    sdf.setTimeZone(TimeZone.getTimeZone("UTC-0"))
    //    val now: Date = new Date
    //     //开始时间
    //
    //    println(sdf.format(new Date()) + "T00:00:00+08:00")
    //    val jedis = RedisClient.pool.getResource
    //    val oldValue = jedis.hget("lcs:live:visit:count", "4253622")
    //    jedis.close()
//    getBetweenDates("2020-04-01", "2020-04-01").foreach(println)
    //    println("2020-09-01 22:00:11.0".substring(0,19))

//
//    val data = jsonParse(
//      """
//        |{
//        |	"code": 0,
//        |	"msg": "成功",
//        |	"data": {
//        |		"live_video": {
//        |			"title": "视频直播",
//        |			"data": [{
//        |				"id": "57691",
//        |				"sequence": "0",
//        |				"title": "指数分化出分歧，注意机构资金新方向",
//        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200403\/20200403013119765aa72cc0f2c6b5ade8868b97b42bf6d776.jpeg",
//        |				"name": "骑牛看熊",
//        |				"p_uid": "1987119657",
//        |				"circle_id": "60151",
//        |				"video_id": "",
//        |				"start_time": "2020-07-08 12:52:41",
//        |				"img_url": "",
//        |				"end_time": "2020-07-08 14:00:00",
//        |				"tag": "1",
//        |				"live_img": "https:\/\/fs.licaishisina.com\/upload\/20200702\/20200702035335145aa72cc0f2c6b5ade8868b97b42bf6d714.jpg",
//        |				"comments": ["被套了", "你好", "666", "军军分享了直播间", "南柯一梦关注了老师,下次开播会收到通知"],
//        |				"router": {
//        |					"type": "video_live_room",
//        |					"relation_id": "60151",
//        |					"video_id": ""
//        |				}
//        |			}, {
//        |				"id": "57676",
//        |				"sequence": "0",
//        |				"title": "巩固牛市位，初期回踩皆良机",
//        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200512\/20200512025313675aa72cc0f2c6b5ade8868b97b42bf6d767.png",
//        |				"name": "李海维",
//        |				"p_uid": "2692158981",
//        |				"circle_id": "61308",
//        |				"video_id": "",
//        |				"start_time": "2020-07-08 12:31:10",
//        |				"img_url": "",
//        |				"end_time": "2020-07-08 13:30:00",
//        |				"tag": "1",
//        |				"live_img": "https:\/\/fs.licaishisina.com\/upload\/20200702\/20200702033201665aa72cc0f2c6b5ade8868b97b42bf6d766.jpg",
//        |				"comments": ["股票池是今天选出来的吗老师", "股票池里是可以做的股票么", "看到了", "老师头像点下，进入主页就可以了", "名字改过两次后 可以用50积分该", "好知道了谢谢", "拭目以待！", " $新力金融(sh600318)$ 老师又放牛了", "股票端木元271关注了老师,下次开播会收到通知", "股票端木元271关注了老师,下次开播会收到通知"],
//        |				"router": {
//        |					"type": "video_live_room",
//        |					"relation_id": "61308",
//        |					"video_id": ""
//        |				}
//        |			}],
//        |			"expire": 1594184073
//        |		},
//        |		"suspension": {
//        |			"title": "悬浮入口",
//        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200708\/20200708040840138d96daad5f97afe0e3034422030bcb6b13.gif",
//        |			"img2": "https:\/\/fs.licaishisina.com\/upload\/20200708\/20200708040844673f48f74c5dd40408ef3303e18d8e07e167.png",
//        |			"route": {
//        |				"type": "goto_wxminipro",
//        |				"type_text": "",
//        |				"relation_id": "gh_0ab266431788",
//        |				"url": "",
//        |				"isTransparentNavigationBar": "0",
//        |				"path": "pages\/goContactAct\/main?act=act061701&bid=10198&type=2&channel=ios&os=ios&deviceid=8B059B1A-F003-4E3C-8A8D-21557C6B33AE&page=suspension"
//        |			}
//        |		},
//        |		"index_type": "index"
//        |	},
//        |	"is_login": 0,
//        |	"run_time": 0.1241340637207,
//        |	"sys_time": "2020-07-08 12:54:35",
//        |	"trace_id": "uirmbndecl6n7usg515ubm6gee"
//        |}
//        |""".stripMargin)
//    val data2 = jsonParse(data.getOrElse("data",""))
//    println(data2.getOrElse("suspension",""))
//val jsonParser = new JSONParser()
//val adArray  = jsonParser.parse(
//  """
//    |[{
//    |			"type": "goto_wxminipro",
//    |			"relation_id": "gh_0ab266431788",
//    |			"title": "3只金股天天领-huaweipay",
//    |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200619\/202006190822485124fbb1d0dc349ff2bc3613a353cedd2f51.jpg",
//    |			"url": "",
//    |			"sequence": "1593772034",
//    |			"status": "0",
//    |			"start_time": "2020-07-03 18:27:30",
//    |			"end_time": "2021-01-01 00:00:00",
//    |			"ver": "",
//    |			"adid": "107160",
//    |			"video_url": "",
//    |			"path": "pages\/goContactAct\/main?act=act061701&bid=10231&type=2",
//    |			"is_video": 0
//    |		}]
//    |""".stripMargin).asInstanceOf[JSONArray].toArray.map( v => if(v == null) "{}" else v.toString)
//    adArray.foreach(println(_))
//    splitDayMinute2.foreach(println(_))

  }

  case class UserBean(name: String, age: Int, Time: Long)

  def msTillTomorrow: Long = {
    val now = new Date()
    val tomorrow = new Date(now.getYear, now.getMonth, now.getDate + 1, 0, 0, 0)
    tomorrow.getTime - now.getTime
  }

  def getBetweenDates(start: String, end: String) = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startData = sdf.parse(start); //定义起始日期
    val endData = sdf.parse(end); //定义结束日期
    var buffer = new ListBuffer[String]

    val tempStart = Calendar.getInstance()
    tempStart.setTime(startData)
    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(endData)

    while (!tempStart.after(tempEnd)) {
      buffer += sdf.format(tempStart.getTime)
      tempStart.add(Calendar.DAY_OF_YEAR, 1)
    }
    buffer.toList
  }


  def readLcsId(): List[String] = {
    var conn: Connection = null
    var stmt: Statement = null
    var result: List[String] = List()

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
            conn = DriverManager.getConnection("jdbc:mysql://rm-2zebtm824um01072v5o.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "lcs_spider_r", "qE1$eB1*mF3}")
//      conn = DriverManager.getConnection("jdbc:mysql://rm-2zebtm824um01072v.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "lcs_spider_r", "qE1$eB1*mF3}")
      stmt = conn.createStatement
      val sql = s"SELECT DISTINCT uid as lcs_id from  lcs_circle_notice "
      val rs: ResultSet = stmt.executeQuery(sql)

      // json数组
      // 遍历ResultSet中的每条数据
      while (rs.next()) {
        result = result.::(rs.getLong("lcs_id") + "")
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

}
