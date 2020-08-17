package cn.yintech.eventLog

import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.TimeUnit

import cn.yintech.esUtil.ESConfig
import cn.yintech.eventLog.SparkReadEsRealTimeCount.{getBetweenDates, jsonParse}
import net.minidev.json.JSONArray
import net.minidev.json.parser.JSONParser
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}

import scala.util.matching.Regex

object SparkReadEsPro_yii2_elk_success {
  def main(args: Array[String]): Unit = {
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

    val spark = SparkSession.builder()
      .appName("SparkReadEsPro_yii2_elk_success")
      .config("hive.exec.dynamic.partition", "true")
      .config("spark.yarn.executor.memoryOverhead","2G")
//            .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    import spark.implicits._
    getBetweenDates(startDt,endDt).foreach( dt => {
      splitDayMinute2.foreach(minutes => {
//      Array(Array(("12:00:00","12:00:30"),("12:00:30","12:01:00"))).foreach(minutes => {
        val value = spark.sparkContext.parallelize(minutes,6)
          .mapPartitions( rdd => {
            rdd.flatMap( v => {
              Thread.sleep(1000)
              println(dt + " " + v._1 +"——"+ dt + " " + v._2)
              esSearchPro_yii2_elk_success1(dt + " " + v._1 , dt + " " + v._2)
                .union(esSearchPro_yii2_elk_success2(dt + " " + v._1 , dt + " " + v._2))
                .union(esSearchPro_yii2_elk_success3(dt + " " + v._1 , dt + " " + v._2))
            })
          })
          .map(v => {
            val jsonObj = jsonParse(v)
            (jsonObj.getOrElse("deviceId",""),
              jsonObj.getOrElse("log_time",""),
              jsonObj.getOrElse("action",""),
              jsonObj.getOrElse("data","")
            )
          }).persist(StorageLevel.MEMORY_AND_DISK)

        // 1.万能弹窗 clientGuide
        val clientGuide = value.filter(_._3 == "app/clientGuide")
          .map(v =>{
            var code = "-1"
            var title = ""
            var g_id = ""
            var index_type = ""
            try {
              val dataJson: Map[String, String] = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data","")
              code = dataJson.getOrElse("code","")
              val data2Json = jsonParse(dataStr)
              g_id = data2Json.getOrElse("g_id","")
              index_type = data2Json.getOrElse("index_type","")
              title = data2Json.getOrElse("title","")
            } catch {
              case e:Exception => println(e.getMessage)
            }
            (v._1,v._2,v._3,v._4,"clientGuide",g_id,title,code,index_type,"")
          }).filter(_._7 != "")
          .toDF("deviceId","log_time","action","data","type","id","title","code","index_type","is_lock")

        // 2.开机闪屏
        val startUpAds = value.filter( v => v._3 == "app/startUpAds" && v._4.contains("adid"))
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
              index_type = data2Json.getOrElse("index_type","")
            } catch {
              case e:Exception => println(e.getMessage)
            }
            if (adStr == ""){
              None
            } else {
              var adArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                adArray  = jsonParser.parse(adStr).asInstanceOf[JSONArray].toArray.map( v => if(v == null) "{}" else v.toString)
              } catch {
                case e:Exception => println(e.getMessage)
              }
              val dataArray = adArray.map(r => {
                var adid = ""
                var title = ""
                try {
                  val rJson = jsonParse(r)
                  adid = rJson.getOrElse("adid", "")
                  title = rJson.getOrElse("title", "")
                } catch {
                  case e:Exception => println(e.getMessage)
                }
                (v._1, v._2, v._3, r, "startUpAds", adid, title, code ,index_type,"")
              })
              dataArray
            }
          }).filter(_._7 != "")
          .toDF("deviceId","log_time","action","data","type","id","title","code","index_type","is_lock")

        // 3.悬浮球 xuanfuqiu
        val index_xuanfuqiu = value.filter( v => v._3 == "app/index" && v._4.contains("悬浮入口"))
          .map(v =>{
            var code = "-1"
            var title = ""
            var suspensionStr = ""
            var index_type = ""
            try{
              val dataJson = jsonParse(v._4)
              val dataStr = dataJson.getOrElse("data","")
              code = dataJson.getOrElse("code","")
              val data2Json = jsonParse(dataStr)
              suspensionStr = data2Json.getOrElse("suspension","")
              index_type = data2Json.getOrElse("index_type","")
              val suspensionJson = jsonParse(suspensionStr)
              title = suspensionJson.getOrElse("title","")
            } catch {
              case e:Exception => println(e.getMessage)
            }
            (v._1,v._2,v._3,suspensionStr,"xuanfuqiu","",title,code,index_type,"")
          })
          .toDF("deviceId","log_time","action","data","type","id","title","code","index_type","is_lock")

        // 4.banner 推广首页banner&TD-首页banner
        val index_banner = value.filter( v => v._3 == "app/index" && v._4.contains("banner"))
          .flatMap(v =>{
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
              case e:Exception => println(e.getMessage)
            }
            if (bannerStr == ""){
              None
            } else {
              var bannerArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                bannerArray  = jsonParser.parse(bannerStr).asInstanceOf[JSONArray].toArray.map( v => if(v == null) "{}" else v.toString)
              } catch {
                case e:Exception => println(e.getMessage)
              }
              val dataArray = bannerArray.map(r => {
                var id = ""
                var title = ""
                try {
                  val rJson = jsonParse(r)
                  id = rJson.getOrElse("id","")
                  title = rJson.getOrElse("title","")
                } catch {
                  case e:Exception => println(e.getMessage)
                }
                (v._1,v._2,v._3,r,"banner",id,title,code,index_type,"")
              })
              dataArray
            }
          }).filter(_._7 != "")
          .toDF("deviceId","log_time","action","data","type","id","title","code","index_type","is_lock")

        // 5.entrance 推广首页金刚位&TD-金刚位
        val index_entrance = value.filter( v => v._3 == "app/index" && v._4.contains("entrance"))
          .flatMap(v =>{
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
              case e:Exception => println(e.getMessage)
            }

            if (entranceStr == ""){
              None
            } else {
              var entranceArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                entranceArray = jsonParser.parse(entranceStr).asInstanceOf[JSONArray].toArray.map( v => if(v == null) "{}" else v.toString)
              } catch {
                case e:Exception => println(e.getMessage)
              }
              val dataArray = entranceArray.toArray.map(_.toString).map(r => {
                var id = ""
                var title = ""
                try {
                  val rJson = jsonParse(r)
                  id = rJson.getOrElse("id", "")
                  title = rJson.getOrElse("title", "")
                } catch {
                  case e:Exception => println(e.getMessage)
                }
                (v._1, v._2, v._3, r, "entrance", id, title, code ,index_type,"")
              })
              dataArray
            }
          }).filter(_._7 != "")
          .toDF("deviceId","log_time","action","data","type","id","title","code","index_type","is_lock")

        // 6.daily_gold_stock 三只金股
        val index_daily_gold_stock = value.filter( v => v._3 == "app/index" && v._4.contains("daily_gold_stock"))
          .flatMap(v =>{
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
              case e:Exception => println(e.getMessage)
            }
            if (daily_gold_stockJsonData == ""){
              None
            } else {
              var daily_gold_stockArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                daily_gold_stockArray = jsonParser.parse(daily_gold_stockJsonData).asInstanceOf[JSONArray].toArray.map( v => if(v == null) "{}" else v.toString)
              } catch {
                case e:Exception => println(e.getMessage)
              }
              val dataArray = daily_gold_stockArray.map(r => {
                var id = ""
                try {
                  val rJson = jsonParse(r)
                  id = rJson.getOrElse("id", "")
                } catch {
                  case e:Exception => println(e.getMessage)
                }
                (v._1, v._2, v._3, r, "daily_gold_stock", id, title, code,index_type,"")
              })
              dataArray
            }
          }).filter(_._7 != "")
          .toDF("deviceId","log_time","action","data","type","id","title","code","index_type","is_lock")

        // 7.channel_daily_gold_stock 三只金股
        val channel_daily_gold_stock = value.filter( v => v._3 == "app/index" && v._4.contains("channel_daily_gold_stock"))
          .flatMap(v =>{
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
              case e:Exception => println(e.getMessage)
            }
            if (daily_gold_stockJsonData == ""){
              None
            } else {
              var daily_gold_stockArray = Array("{}")
              try {
                val jsonParser = new JSONParser()
                daily_gold_stockArray = jsonParser.parse(daily_gold_stockJsonData).asInstanceOf[JSONArray].toArray.map( v => if(v == null) "{}" else v.toString)
              } catch {
                case e:Exception => println(e.getMessage)
              }
              val dataArray = daily_gold_stockArray.map(r => {
                var id = ""
                try {
                  val rJson = jsonParse(r)
                  id = rJson.getOrElse("id", "")
                } catch {
                  case e:Exception => println(e.getMessage)
                }
                (v._1, v._2, v._3, r, "channel_daily_gold_stock", id, title, code,index_type,is_lock)
              })
              dataArray
            }
          }).filter(_._7 != "")
          .toDF("deviceId","log_time","action","data","type","id","title","code","index_type","is_lock")

//        val result = clientGuide.union(startUpAds).union(index_xuanfuqiu).union(index_banner).union(index_entrance).union(index_daily_gold_stock)
//            .toDF("deviceId","log_time","action","data","type","id","title","code")


        clientGuide.createOrReplaceGlobalTempView("table1")
        startUpAds.createOrReplaceGlobalTempView("table2")
        index_xuanfuqiu.createOrReplaceGlobalTempView("table3")
        index_banner.createOrReplaceGlobalTempView("table4")
        index_entrance.createOrReplaceGlobalTempView("table5")
        index_daily_gold_stock.createOrReplaceGlobalTempView("table6")
        channel_daily_gold_stock.createOrReplaceGlobalTempView("table7")

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
      })

    })
    spark.stop()

//    esSearchPro_yii2_elk_success1("2020-07-09 12:00:00","2020-07-09 12:01:00").take(10).foreach(println(_))

  }

  def esSearchPro_yii2_elk_success1( startTime: String, endTime: String): List[String] = {
    val client = ESConfig.client()
    val boolBuilder = QueryBuilders.boolQuery()
    val sourceBuilder = new SearchSourceBuilder()
    //创建一个socket
    val rangeQueryBuilder = QueryBuilders.rangeQuery("log_time.keyword") //新建range条件
    rangeQueryBuilder.gte(startTime)
    rangeQueryBuilder.lt(endTime)
    boolBuilder.must(rangeQueryBuilder)
    boolBuilder.must(QueryBuilders.matchQuery("action.keyword", "app/clientGuide"))
//    boolBuilder.must(QueryBuilders.nestedQuery("data",QueryBuilders.matchQuery("data.code", "0"))

//    val sortBuilder = SortBuilders.fieldSort("logtime").order(SortOrder.ASC) // 排训规则

    sourceBuilder.query(boolBuilder) //设置查询，可以是任何类型的QueryBuilder。
    sourceBuilder.from(0) //设置确定结果要从哪个索引开始搜索的from选项，默认为0
    sourceBuilder.size(10000) //设置确定搜素命中返回数的size选项，默认为10
    sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)) //设置一个可选的超时，控制允许搜索的时间。
//    sourceBuilder.sort(sortBuilder)

    sourceBuilder.fetchSource( Array[String]("log_time","deviceId","action","data"), Array[String]()) //第一个是获取字段，第二个是过滤的字段，默认获取全部
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

  def esSearchPro_yii2_elk_success2( startTime: String, endTime: String): List[String] = {
    val client = ESConfig.client()
    val boolBuilder = QueryBuilders.boolQuery()
    val sourceBuilder = new SearchSourceBuilder()
    //创建一个socket
    val rangeQueryBuilder = QueryBuilders.rangeQuery("log_time.keyword") //新建range条件
    rangeQueryBuilder.gte(startTime)
    rangeQueryBuilder.lt(endTime)
    boolBuilder.must(rangeQueryBuilder)
    boolBuilder.must(QueryBuilders.matchQuery("action.keyword", "app/index"))
    //    boolBuilder.must(QueryBuilders.nestedQuery("data",QueryBuilders.matchQuery("data.code", "0"))

    //    val sortBuilder = SortBuilders.fieldSort("logtime").order(SortOrder.ASC) // 排训规则

    sourceBuilder.query(boolBuilder) //设置查询，可以是任何类型的QueryBuilder。
    sourceBuilder.from(0) //设置确定结果要从哪个索引开始搜索的from选项，默认为0
    sourceBuilder.size(10000) //设置确定搜素命中返回数的size选项，默认为10
    sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)) //设置一个可选的超时，控制允许搜索的时间。
    //    sourceBuilder.sort(sortBuilder)

    sourceBuilder.fetchSource( Array[String]("log_time","deviceId","action","data"), Array[String]()) //第一个是获取字段，第二个是过滤的字段，默认获取全部
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

  def esSearchPro_yii2_elk_success3( startTime: String, endTime: String): List[String] = {
    val client = ESConfig.client()
    val boolBuilder = QueryBuilders.boolQuery()
    val sourceBuilder = new SearchSourceBuilder()
    //创建一个socket
    val rangeQueryBuilder = QueryBuilders.rangeQuery("log_time.keyword") //新建range条件
    rangeQueryBuilder.gte(startTime)
    rangeQueryBuilder.lt(endTime)
    boolBuilder.must(rangeQueryBuilder)
    boolBuilder.must(QueryBuilders.matchQuery("action.keyword", "app/startUpAds"))
    //    boolBuilder.must(QueryBuilders.nestedQuery("data",QueryBuilders.matchQuery("data.code", "0"))

    //    val sortBuilder = SortBuilders.fieldSort("logtime").order(SortOrder.ASC) // 排训规则

    sourceBuilder.query(boolBuilder) //设置查询，可以是任何类型的QueryBuilder。
    sourceBuilder.from(0) //设置确定结果要从哪个索引开始搜索的from选项，默认为0
    sourceBuilder.size(10000) //设置确定搜素命中返回数的size选项，默认为10
    sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)) //设置一个可选的超时，控制允许搜索的时间。
    //    sourceBuilder.sort(sortBuilder)

    sourceBuilder.fetchSource( Array[String]("log_time","deviceId","action","data"), Array[String]()) //第一个是获取字段，第二个是过滤的字段，默认获取全部
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


  def splitDayMinute () = {
    val sdf = new SimpleDateFormat("HH:mm:ss")
    val minutes1 = (0 until 360).flatMap( i => {
      if (i == 1439)
        Array((sdf.format(-28800000+i*1000*60),"23:59:30"),
          ("23:59:30","24:00:00"))
      else
        Array((sdf.format(-28800000+i*1000*60),sdf.format(-28800000+(i+1)*1000*60-30000)),
          (sdf.format(-28800000+(i+1)*1000*60-30000),sdf.format(-28800000+(i+1)*1000*60)))
    })
    val minutes2 = (360 until 720).flatMap( i => {
      if (i == 1439)
        Array((sdf.format(-28800000+i*1000*60),"23:59:30"),
          ("23:59:30","24:00:00"))
      else
        Array((sdf.format(-28800000+i*1000*60),sdf.format(-28800000+(i+1)*1000*60-30000)),
          (sdf.format(-28800000+(i+1)*1000*60-30000),sdf.format(-28800000+(i+1)*1000*60)))
    })
    val minutes3 = (720 until 1080).flatMap( i => {
      if (i == 1439)
        Array((sdf.format(-28800000+i*1000*60),"23:59:30"),
          ("23:59:30","24:00:00"))
      else
        Array((sdf.format(-28800000+i*1000*60),sdf.format(-28800000+(i+1)*1000*60-30000)),
          (sdf.format(-28800000+(i+1)*1000*60-30000),sdf.format(-28800000+(i+1)*1000*60)))
    })
    val minutes4 = (1080 until 1440).flatMap( i => {
      if (i == 1439)
        Array((sdf.format(-28800000+i*1000*60),"23:59:30"),
          ("23:59:30","24:00:00"))
      else
        Array((sdf.format(-28800000+i*1000*60),sdf.format(-28800000+(i+1)*1000*60-30000)),
          (sdf.format(-28800000+(i+1)*1000*60-30000),sdf.format(-28800000+(i+1)*1000*60)))
    })
    Array(minutes1,minutes2,minutes3,minutes4)
  }

  def splitDayMinute2 () = {
    val sdf = new SimpleDateFormat("HH:mm:ss")
    val minutes1 = (0 until 360).flatMap( i => {
        Array((sdf.format(-28800000+i*1000*60),sdf.format(-28800000+(i+1)*1000*60)))
    })
    val minutes2 = (360 until 720).flatMap( i => {
        Array((sdf.format(-28800000+i*1000*60),sdf.format(-28800000+(i+1)*1000*60)))
    })
    val minutes3 = (720 until 1080).flatMap( i => {
        Array((sdf.format(-28800000+i*1000*60),sdf.format(-28800000+(i+1)*1000*60)))
    })
    val minutes4 = (1080 until 1440).flatMap( i => {
      if (i == 1439)
        Array((sdf.format(-28800000+i*1000*60),"24:00:00"))
      else
        Array((sdf.format(-28800000+i*1000*60),sdf.format(-28800000+(i+1)*1000*60)))
    })
    Array(minutes1,minutes2,minutes3,minutes4)
  }

}
