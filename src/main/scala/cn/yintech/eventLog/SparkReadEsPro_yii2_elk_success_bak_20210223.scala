package cn.yintech.eventLog

import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.TimeUnit

import cn.yintech.esUtil.ESConfig
import cn.yintech.eventLog.SparkReadEsRealTimeCount.{getBetweenDates, jsonParse}
import net.minidev.json.JSONArray
import net.minidev.json.parser.JSONParser
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.util.matching.Regex

/**
 * 素材下发统计，数据抽取（素材点击统计取埋点数据，使用hivesql脚本，无spark代码）
 * Spark抽取ES数据pro-yii2-elk-success* /doc
 */
object SparkReadEsPro_yii2_elk_success_bak_20210223 {
  def main(args: Array[String]): Unit = {
    //  获取日期分区参数
    require(!(args == null || args.length != 2), "Required 'startDt & endDt' args")
    val pattern = new Regex("\\d{4}[-]\\d{2}[-]\\d{2}")
    val dateSome1 = pattern findFirstIn args(0)
    val dateSome2 = pattern findFirstIn args(1)
    require(dateSome1.isDefined, s"Required PARTITION args like 'yyyy-mm-dd' but find ${args(0)}")
    require(dateSome2.isDefined, s"Required PARTITION args like 'yyyy-mm-dd' but find ${args(1)}")
    val startDt = dateSome1.get // 实际使用yyyy-mm-dd格式日期
    val endDt = dateSome2.get // 实际使用yyyy-mm-dd格式日期
    println("startDate dt : " + startDt + ",endDate dt : " + endDt)

    val spark = SparkSession.builder()
      .appName("SparkReadEsPro_yii2_elk_success")
      .config("hive.exec.dynamic.partition", "true")
      .config("spark.yarn.executor.memoryOverhead", "2G")
      //            .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    import spark.implicits._
    getBetweenDates(startDt, endDt).foreach(dt => {
      splitDayMinute2.foreach(minutes => {
        //      Array(Array(("12:00:00","12:00:30"),("12:00:30","12:01:00"))).foreach(minutes => {
        val value = spark.sparkContext.parallelize(minutes, 6)
          .mapPartitions(rdd => {
            rdd.flatMap(v => {
              Thread.sleep(1000)
              println(dt + " " + v._1 + "——" + dt + " " + v._2)
              esSearchPro_yii2_elk_success(dt + " " + v._1, dt + " " + v._2, "app/index")
                .union(esSearchPro_yii2_elk_success(dt + " " + v._1, dt + " " + v._2, "app/clientGuide"))
                .union(esSearchPro_yii2_elk_success(dt + " " + v._1, dt + " " + v._2, "app/startUpAds"))
                .union(esSearchPro_yii2_elk_success(dt + " " + v._1, dt + " " + v._2, "app/newInfoCircle"))
                .union(esSearchPro_yii2_elk_success(dt + " " + v._1, dt + " " + v._2, "app/tuyereHotList"))
                .union(esSearchPro_yii2_elk_success(dt + " " + v._1, dt + " " + v._2, "app/homePage"))
                .union(esSearchPro_yii2_elk_success(dt + " " + v._1, dt + " " + v._2, "api/indexPlateInfo"))
                .union(esSearchPro_yii2_elk_success(dt + " " + v._1, dt + " " + v._2, "app/compositeVideo"))
            })
          })
          .map(v => {
            val jsonObj = jsonParse(v)
            (jsonObj.getOrElse("deviceId", ""),
              jsonObj.getOrElse("log_time", ""),
              jsonObj.getOrElse("action", ""),
              jsonObj.getOrElse("data", "")
            )
          }).persist(StorageLevel.MEMORY_AND_DISK)

        // 1.万能弹窗 clientGuide
        val clientGuide = value.filter(_._3 == "app/clientGuide")
          .map(v => {
            var code = "-1"
            var title = ""
            var g_id = ""
            var index_type = ""
            try {
              val dataJson: Map[String, String] = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(dataStr)
              g_id = data2Json.getOrElse("g_id", "")
              index_type = data2Json.getOrElse("index_type", "")
              title = data2Json.getOrElse("title", "")
            } catch {
              case e: Exception => println(e.getMessage)
            }
            (v._1, v._2, v._3, v._4, "clientGuide", g_id, title, code, index_type, "")
          }).filter(_._7 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 2.开机闪屏
        val startUpAds = value.filter(v => v._3 == "app/startUpAds" && v._4.contains("adid"))
          .flatMap(v => {
            var code = "-1"
            var adStr = ""
            var index_type = ""
            try {
              val dataJson = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(dataStr)
              adStr = data2Json.getOrElse("ad", "")
              index_type = data2Json.getOrElse("index_type", "")
            } catch {
              case e: Exception => println(e.getMessage)
            }
            if (adStr == "") {
              None
            } else {
              var adArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                adArray = jsonParser.parse(adStr).asInstanceOf[JSONArray].toArray.map(v => if (v == null) "{}" else v.toString)
              } catch {
                case e: Exception => println(e.getMessage)
              }
              val dataArray = adArray.map(r => {
                var adid = ""
                var title = ""
                try {
                  val rJson = jsonParse(r)
                  adid = rJson.getOrElse("adid", "")
                  title = rJson.getOrElse("title", "")
                } catch {
                  case e: Exception => println(e.getMessage)
                }
                (v._1, v._2, v._3, r, "startUpAds", adid, title, code, index_type, "")
              })
              dataArray
            }
          }).filter(_._7 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 3.悬浮球 xuanfuqiu
        //        val index_xuanfuqiu = value.filter( v => v._3 == "app/index" && v._4.contains("悬浮入口"))
        //        val index_xuanfuqiu = value.filter( v => v._3 == "app/index" && (v._4.contains("8月金股") || v._4.contains("领犇股")) )
        val index_xuanfuqiu = value.filter(v => v._3 == "app/index" && v._4.contains("\"suspension\""))
          .map(v => {
            var code = "-1"
            var title = ""
            var suspensionStr = ""
            var index_type = ""
            try {
              val dataJson = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(dataStr)
              suspensionStr = data2Json.getOrElse("suspension", "")
              index_type = data2Json.getOrElse("index_type", "")
              val suspensionJson = jsonParse(suspensionStr)
              title = suspensionJson.getOrElse("title", "")
            } catch {
              case e: Exception => println(e.getMessage)
            }
            (v._1, v._2, v._3, suspensionStr, "xuanfuqiu", "", title, code, "", "")
          })
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 4.banner 推广首页banner&TD-首页banner
        val index_banner = value.filter(v => v._3 == "app/index" && v._4.contains("banner"))
          .flatMap(v => {
            var bannerStr = ""
            var code = "-1"
            var index_type = ""
            try {
              val dataJson = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(dataStr)
              bannerStr = data2Json.getOrElse("banner", "")
              index_type = data2Json.getOrElse("index_type", "")
            } catch {
              case e: Exception => println(e.getMessage)
            }
            if (bannerStr == "") {
              None
            } else {
              var bannerArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                bannerArray = jsonParser.parse(bannerStr).asInstanceOf[JSONArray].toArray.map(v => if (v == null) "{}" else v.toString)
              } catch {
                case e: Exception => println(e.getMessage)
              }
              val dataArray = bannerArray.map(r => {
                var id = ""
                var title = ""
                try {
                  val rJson = jsonParse(r)
                  id = rJson.getOrElse("id", "")
                  title = rJson.getOrElse("title", "")
                } catch {
                  case e: Exception => println(e.getMessage)
                }
                (v._1, v._2, v._3, r, "banner", id, title, code, index_type, "")
              })
              dataArray
            }
          }).filter(_._7 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 5.entrance 推广首页金刚位&TD-金刚位
        val index_entrance = value.filter(v => v._3 == "app/index" && v._4.contains("entrance"))
          .flatMap(v => {
            var code = "-1"
            var entranceStr = ""
            var index_type = ""
            try {
              val dataJson = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(dataStr)
              entranceStr = data2Json.getOrElse("entrance", "")
              index_type = data2Json.getOrElse("index_type", "")
            } catch {
              case e: Exception => println(e.getMessage)
            }

            if (entranceStr == "") {
              None
            } else {
              var entranceArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                entranceArray = jsonParser.parse(entranceStr).asInstanceOf[JSONArray].toArray.map(v => if (v == null) "{}" else v.toString)
              } catch {
                case e: Exception => println(e.getMessage)
              }
              val dataArray = entranceArray.toArray.map(_.toString).map(r => {
                var id = ""
                var title = ""
                try {
                  val rJson = jsonParse(r)
                  id = rJson.getOrElse("id", "")
                  title = rJson.getOrElse("title", "")
                } catch {
                  case e: Exception => println(e.getMessage)
                }
                (v._1, v._2, v._3, r, "entrance", id, title, code, index_type, "")
              })
              dataArray
            }
          }).filter(_._7 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 6.daily_gold_stock 三只金股
        val index_daily_gold_stock = value.filter(v => v._3 == "app/index" && v._4.contains("daily_gold_stock"))
          .flatMap(v => {
            var code = "-1"
            var daily_gold_stockJsonData = ""
            var title = ""
            var index_type = ""
            try {
              val dataJson = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(dataStr)
              val daily_gold_stockStr = data2Json.getOrElse("daily_gold_stock", "")
              index_type = data2Json.getOrElse("index_type", "")
              val daily_gold_stockJson = jsonParse(daily_gold_stockStr)
              title = daily_gold_stockJson.getOrElse("title", "")
              daily_gold_stockJsonData = daily_gold_stockJson.getOrElse("data", "")
            } catch {
              case e: Exception => println(e.getMessage)
            }
            if (daily_gold_stockJsonData == "") {
              None
            } else {
              var daily_gold_stockArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                daily_gold_stockArray = jsonParser.parse(daily_gold_stockJsonData).asInstanceOf[JSONArray].toArray.map(v => if (v == null) "{}" else v.toString)
              } catch {
                case e: Exception => println(e.getMessage)
              }
              val dataArray = daily_gold_stockArray.map(r => {
                var id = ""
                try {
                  val rJson = jsonParse(r)
                  id = rJson.getOrElse("id", "")
                } catch {
                  case e: Exception => println(e.getMessage)
                }
                (v._1, v._2, v._3, r, "daily_gold_stock", id, title, code, index_type, "")
              })
              dataArray
            }
          }).filter(_._7 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 7.channel_daily_gold_stock 三只金股
        val channel_daily_gold_stock = value.filter(v => v._3 == "app/index" && v._4.contains("channel_daily_gold_stock"))
          .flatMap(v => {
            var code = "-1"
            var daily_gold_stockJsonData = ""
            var title = ""
            var index_type = ""
            var is_lock = ""
            try {
              val dataJson = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(dataStr)
              val daily_gold_stockStr = data2Json.getOrElse("channel_daily_gold_stock", "")
              index_type = data2Json.getOrElse("index_type", "")
              val daily_gold_stockJson = jsonParse(daily_gold_stockStr)
              title = daily_gold_stockJson.getOrElse("title", "")
              is_lock = daily_gold_stockJson.getOrElse("is_lock", "")
              daily_gold_stockJsonData = daily_gold_stockJson.getOrElse("data", "")
            } catch {
              case e: Exception => println(e.getMessage)
            }
            if (daily_gold_stockJsonData == "") {
              None
            } else {
              var daily_gold_stockArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                daily_gold_stockArray = jsonParser.parse(daily_gold_stockJsonData).asInstanceOf[JSONArray].toArray.map(v => if (v == null) "{}" else v.toString)
              } catch {
                case e: Exception => println(e.getMessage)
              }
              val dataArray = daily_gold_stockArray.map(r => {
                var id = ""
                try {
                  val rJson = jsonParse(r)
                  id = rJson.getOrElse("id", "")
                } catch {
                  case e: Exception => println(e.getMessage)
                }
                (v._1, v._2, v._3, r, "channel_daily_gold_stock", id, title, code, index_type, is_lock)
              })
              dataArray
            }
          }).filter(_._7 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 8.live_xuanfuqiu 直播间悬浮球
        val live_xuanfuqiu = value.filter(v => v._3 == "app/newInfoCircle" && v._4.contains("red_packet"))
          .map(v => {
            var code = "-1"
            var red_packet = ""
            var title = ""
            var name = ""
            var p_uid = ""
            try {
              val dataJson = jsonParse(v._4)
              val data2Str = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(data2Str)
              val planner = data2Json.getOrElse("planner", "")
              val circle = data2Json.getOrElse("circle", "")

              val plannerJson = jsonParse(planner)
              val planner_info = plannerJson.getOrElse("planner_info", "")
              val planner_infoJson = jsonParse(planner_info)
              p_uid = planner_infoJson.getOrElse("p_uid", "")
              name = planner_infoJson.getOrElse("name", "")

              val circleJson = jsonParse(circle)
              red_packet = circleJson.getOrElse("red_packet", "")

            } catch {
              case e: Exception => println(e.getMessage)
            }
            if (red_packet == "") {
              ("", "", "", "", "", "", "", "", "", "")
            } else {
              try {
                val red_packetJson = jsonParse(red_packet)
                title = red_packetJson.getOrElse("title", "")
              } catch {
                case e: Exception => println(e.getMessage)
              }

              (v._1, v._2, v._3, red_packet, "live_xuanfuqiu", p_uid, title, code, "", "")
            }
          }).filter(v => v._7 != ""
          && Array("1625572687", "71585119812862").contains(v._6)) // 暂只抽取两位老师的直播间
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 9. index_tuyereHotList 首页_推广_风口热点
        val index_tuyereHotList = value.filter(v => v._3 == "app/tuyereHotList")
          .map(v => {
            var code = "-1"
            var data = ""
            try {
              val dataJson = jsonParse(v._4)
              val data2Str = dataJson.getOrElse("data", "")
              data = data2Str
              code = dataJson.getOrElse("code", "")

            } catch {
              case e: Exception => println(e.getMessage)
            }

            (v._1, v._2, v._3, data, "index_tuyereHotList", "", null, code, "", "")

          })
          //          .filter( v => v._7 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 10.index_special_stocks 首页_推广_特色选股
        val index_special_stocks = value.filter(v => v._3 == "app/index" && v._4.contains("index_special_stocks"))
          .map(v => {
            var code = "-1"
            var data = ""
            var index_type = ""
            var is_lock = ""
            try {
              val dataJson = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(dataStr)
              val index_special_stocksStr = data2Json.getOrElse("index_special_stocks", "")
              index_type = data2Json.getOrElse("index_type", "")
              val index_special_stocksJson = jsonParse(index_special_stocksStr)
              is_lock = index_special_stocksJson.getOrElse("is_unlock", "")
              data = index_special_stocksStr
            } catch {
              case e: Exception => println(e.getMessage)
            }
            (v._1, v._2, v._3, data, "index_special_stocks", "", null, code, index_type, is_lock)
          })
          //          .filter(_._7 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 11. index_market 首页_推广_市场头条
        val index_market = value.filter(v => v._3 == "app/homePage" && v._4.contains("recommend"))
          .map(v => {
            var code = "-1"
            var data = ""
            try {
              val dataJson = jsonParse(v._4)
              val data2Str = dataJson.getOrElse("data", "")
              data = data2Str
              code = dataJson.getOrElse("code", "")

            } catch {
              case e: Exception => println(e.getMessage)
            }

            (v._1, v._2, v._3, data, "index_market", "", null, code, "", "")

          })
          //          .filter( v => v._7 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 12. index_market_headlines 首页_推广_市场头条_更多头条
        val index_market_headlines = value.filter(v => v._3 == "app/index" && v._4.contains("market_headlines"))
          .map(v => {
            var code = "-1"
            var data = ""
            try {
              val dataJson = jsonParse(v._4)
              val data2Str = dataJson.getOrElse("data", "")
              data = data2Str
              code = dataJson.getOrElse("code", "")

            } catch {
              case e: Exception => println(e.getMessage)
            }

            (v._1, v._2, v._3, data, "index_market_headlines", "", null, code, "", "")

          })
          //          .filter( v => v._7 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 13. tuyereHotList_button 风口热点_问答按钮
        val tuyereHotList_button = value.filter(v => v._3 == "api/indexPlateInfo" && v._4.contains("route"))
          .map(v => {
            var code = "-1"
            var data = ""
            try {
              val dataJson = jsonParse(v._4)
              val data2Str = dataJson.getOrElse("data", "")
              data = data2Str
              code = dataJson.getOrElse("code", "")

            } catch {
              case e: Exception => println(e.getMessage)
            }

            (v._1, v._2, v._3, data, "tuyereHotList_button", "", null, code, "", "")

          })
          //          .filter( v => v._7 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 14.index_top_banner 首页_顶部主banner
        val index_top_banner = value.filter(v => v._3 == "app/index" && v._4.contains("index_banner"))
          .map(f = v => {
            var code = "-1"
            var data = ""
            var title = ""
            var index_bannerStr = ""
            try {
              val dataJson = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(dataStr)
              index_bannerStr = data2Json.getOrElse("index_banner", "")
            } catch {
              case e: Exception => println(e.getMessage)
            }
            if (index_bannerStr == null || index_bannerStr == "" || index_bannerStr == "null") {
              ("", "", "", "", "", "", "", "", "", "")
            } else {
              val index_bannerJson = jsonParse(index_bannerStr)
              title = index_bannerJson.getOrElse("title", "")
              data = index_bannerStr
              (v._1, v._2, v._3, data, "index_top_banner", "", title, code, "", "")
            }
          })
          .filter(v => v._5 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 15.live_tab_recommend_banner 直播tab_推荐_banner
        val live_tab_recommend_banner = value.filter(v => v._3 == "app/compositeVideo" && v._4.contains("banner"))
          .flatMap(v => {
            var code = "-1"
            var bannerStr = ""
            try {
              val dataJson = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(dataStr)
              bannerStr = data2Json.getOrElse("banner", "")

            } catch {
              case e: Exception => println(e.getMessage)
            }
            if (bannerStr == null || bannerStr == "" || bannerStr == "null") {
              None
            } else {
              var bannerArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                bannerArray = jsonParser.parse(bannerStr).asInstanceOf[JSONArray].toArray.map(v => if (v == null) "{}" else v.toString)
              } catch {
                case e: Exception => println(e.getMessage)
              }
              val dataArray = bannerArray.map(r => {
                var id = ""
                var title = ""
                try {
                  val rJson = jsonParse(r)
                  id = rJson.getOrElse("id", "")
                  title = rJson.getOrElse("title", "")
                } catch {
                  case e: Exception => println(e.getMessage)
                }
                (v._1, v._2, v._3, r, "live_tab_recommend_banner", id, title, code, "", "")
              })
              dataArray
            }
          }).filter(_._5 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")

        // 16.lcs_main_banner 理财师主页_banner
        val lcs_main_banner = value.filter(v => v._3 == "app/newInfoCircle" && v._4.contains("banner"))
          .flatMap(v => {
            var code = "-1"
            var bannerStr = ""
            try {
              val dataJson = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data", "")
              code = dataJson.getOrElse("code", "")
              val data2Json = jsonParse(dataStr)
              bannerStr = data2Json.getOrElse("banner", "")

            } catch {
              case e: Exception => println(e.getMessage)
            }
            if (bannerStr == null || bannerStr == "" || bannerStr == "null") {
              None
            } else {
              var bannerArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                bannerArray = jsonParser.parse(bannerStr).asInstanceOf[JSONArray].toArray.map(v => if (v == null) "{}" else v.toString)
              } catch {
                case e: Exception => println(e.getMessage)
              }
              val dataArray = bannerArray.map(r => {
                var id = ""
                var title = ""
                try {
                  val rJson = jsonParse(r)
                  id = rJson.getOrElse("id", "")
                  title = rJson.getOrElse("title", "")
                } catch {
                  case e: Exception => println(e.getMessage)
                }
                (v._1, v._2, v._3, r, "lcs_main_banner", id, title, code, "", "")
              })
              dataArray
            }
          }).filter(_._5 != "")
          .toDF("deviceId", "log_time", "action", "data", "type", "id", "title", "code", "index_type", "is_lock")


        clientGuide.createOrReplaceGlobalTempView("table1")
        startUpAds.createOrReplaceGlobalTempView("table2")
        index_xuanfuqiu.createOrReplaceGlobalTempView("table3")
        index_banner.createOrReplaceGlobalTempView("table4")
        index_entrance.createOrReplaceGlobalTempView("table5")
        index_daily_gold_stock.createOrReplaceGlobalTempView("table6")
        channel_daily_gold_stock.createOrReplaceGlobalTempView("table7")
        live_xuanfuqiu.createOrReplaceGlobalTempView("table8")

        index_tuyereHotList.createOrReplaceGlobalTempView("table9")
        index_special_stocks.createOrReplaceGlobalTempView("table10")
        index_market.createOrReplaceGlobalTempView("table11")
        index_market_headlines.createOrReplaceGlobalTempView("table12")
        tuyereHotList_button.createOrReplaceGlobalTempView("table13")

        index_top_banner.createOrReplaceGlobalTempView("table14")
        live_tab_recommend_banner.createOrReplaceGlobalTempView("table15")
        lcs_main_banner.createOrReplaceGlobalTempView("table16")

        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table2 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table1 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table3 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table4 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table5 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table6 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table7 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table8 ")

        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table9 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table10 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table11 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table12 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table13 ")

        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table14 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table15 ")
        spark.sql(s"insert into ods.ods_es_Pro_yii2_elk_success_2_di partition(dt='$dt')" +
          " select * from global_temp.table16 ")


      })

    })
    spark.stop()

    //    esSearchPro_yii2_elk_success1("2020-07-09 12:00:00","2020-07-09 12:01:00").take(10).foreach(println(_))

  }


  // 按起始时间、action.keyword搜索ES
  def esSearchPro_yii2_elk_success(startTime: String, endTime: String, action_keyword: String): List[String] = {
    val client = ESConfig.client()
    val boolBuilder = QueryBuilders.boolQuery()
    val sourceBuilder = new SearchSourceBuilder()
    //创建一个socket
    val rangeQueryBuilder = QueryBuilders.rangeQuery("log_time.keyword") //新建range条件
    rangeQueryBuilder.gte(startTime)
    rangeQueryBuilder.lt(endTime)
    boolBuilder.must(rangeQueryBuilder)
    boolBuilder.must(QueryBuilders.matchQuery("action.keyword", action_keyword))
    //    boolBuilder.must(QueryBuilders.nestedQuery("data",QueryBuilders.matchQuery("data.code", "0"))

    //    val sortBuilder = SortBuilders.fieldSort("logtime").order(SortOrder.ASC) // 排训规则

    sourceBuilder.query(boolBuilder) //设置查询，可以是任何类型的QueryBuilder。
    sourceBuilder.from(0) //设置确定结果要从哪个索引开始搜索的from选项，默认为0
    sourceBuilder.size(10000) //设置确定搜素命中返回数的size选项，默认为10
    sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)) //设置一个可选的超时，控制允许搜索的时间。
    //    sourceBuilder.sort(sortBuilder)

    sourceBuilder.fetchSource(Array[String]("log_time", "deviceId", "action", "data"), Array[String]()) //第一个是获取字段，第二个是过滤的字段，默认获取全部
    val searchRequest = new SearchRequest("pro-yii2-elk-success*") //索引
    searchRequest.types("doc") //类型
    searchRequest.source(sourceBuilder)

    //    val searchResponse = client.search(searchRequest)
    //    val hits = searchResponse.getHits.getHits

    val searchHits: util.List[SearchHit] = ESConfig.scrollSearchAll(client, 10L, searchRequest)

    System.out.println("esSearch total hits : " + searchHits.size)
    client.close()
    import scala.collection.JavaConverters._
    searchHits.asScala.map(_.getSourceAsString).toList

  }

  /**
   * 限流操作，ES出口流量限制防止拉数据超时，按分钟拉取数据
   *
   * @return
   */
  def splitDayMinute2() = {
    val sdf = new SimpleDateFormat("HH:mm:ss")
    val minutes1 = (0 until 360).flatMap(i => {
      Array((sdf.format(-28800000 + i * 1000 * 60), sdf.format(-28800000 + (i + 1) * 1000 * 60)))
    })
    val minutes2 = (360 until 720).flatMap(i => {
      Array((sdf.format(-28800000 + i * 1000 * 60), sdf.format(-28800000 + (i + 1) * 1000 * 60)))
    })
    val minutes3 = (720 until 1080).flatMap(i => {
      Array((sdf.format(-28800000 + i * 1000 * 60), sdf.format(-28800000 + (i + 1) * 1000 * 60)))
    })
    val minutes4 = (1080 until 1440).flatMap(i => {
      if (i == 1439)
        Array((sdf.format(-28800000 + i * 1000 * 60), "24:00:00"))
      else
        Array((sdf.format(-28800000 + i * 1000 * 60), sdf.format(-28800000 + (i + 1) * 1000 * 60)))
    })
    Array(minutes1, minutes2, minutes3, minutes4)
  }

}
