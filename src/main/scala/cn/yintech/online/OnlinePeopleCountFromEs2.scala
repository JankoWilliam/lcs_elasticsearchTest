package cn.yintech.online

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.{ParseException, SimpleDateFormat}
import java.util
import java.util.concurrent.TimeUnit
import java.util.{ArrayList, Date, List, TimeZone}

import cn.yintech.esUtil.ESConfig
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders, RangeQueryBuilder}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.{AggregationBuilder, AggregationBuilders}
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}


object OnlinePeopleCountFromEs2 {
  def main(args: Array[String]): Unit = {

    //配置对象
    val conf = new SparkConf().setAppName("OnlinePeople") //.setMaster("local[2]")
    conf.set("spark.default.parallelism", "10")
      //开启反压
      .set("spark.streaming.backpressure.enabled","true")
      .set("spark.streaming.backpressure.pid.minrate","0.001")
    //    conf.set("spark.driver.allowMultipleContexts","true")
    //      conf.set(ConfigurationOptions.ES_PORT, "9200")
    //      conf.set(ConfigurationOptions.ES_NET_PROXY_HTTP_HOST,"localhost")
    //      conf.set(ConfigurationOptions.ES_NODES_WAN_ONLY, "true")
    //      conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "true")
    //      conf.set(ConfigurationOptions.ES_NODES_DISCOVERY, "false")
    //      conf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "esUser")
    //      conf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "esPwd")

    //      conf.set("spark.streaming.stopGracefullyOnShutdown","true")
    //      conf.set("es.index.auto.create", "true")
    //      conf.set("es.nodes", "127.0.0.1")
    //      conf.set("es.port", "9200")

    //
    //      conf.set("es.write.rest.error.handlers", "ignoreConflict")
    //      conf.set("es.write.rest.error.handler.ignoreConflict", "com.jointsky.bigdata.handler.IgnoreConflictsHandler")
    //


    val ssc = new StreamingContext(conf, Seconds(10))
    //    ssc.checkpoint("./OnlinePeopleCountFromEs")
    ssc.checkpoint("hdfs:///user/licaishi/OnlinePeopleCountFromEs")

    val lineDStream = ssc.receiverStream(new CustomReceiverFromEs())
    //逻辑处理

    val data = lineDStream.map(line => {
      val dataMap = jsonParse(line)
      val is_online = dataMap.getOrElse("is_online", "0")
      (dataMap.getOrElse("extra_id", ""),
        if (dataMap.getOrElse("uid", "") != "") dataMap.getOrElse("uid", "") else dataMap.getOrElse("device_id", ""),
        dataMap.getOrElse("type", ""),
        if (is_online == "") "0" else is_online,
        dataMap.getOrElse("c_time", ""),
        dataMap.getOrElse("action", ""),
        dataMap.getOrElse("data", ""),
        dataMap.getOrElse("content", ""),
        dataMap.getOrElse("name", ""),
        dataMap.getOrElse("relation_id", ""),
        dataMap.getOrElse("title", ""),
        dataMap.getOrElse("points", ""),
        dataMap.getOrElse("device_id_count", "")
      )
    }).persist(StorageLevel.MEMORY_AND_DISK)

    val realTimeData = data
      // extra_id和uid非空且type为三种制定类型
      .filter(v => v._1 != "" && v._1 != null && v._1 != "null" && v._2 != "" && v._4 == "1" && (v._3 == "圈子视频直播" || v._3 == "圈子文字直播" || v._3 == "大直播"))
      // (extra_id,文字直播/视频直播/大直播,uid,c_time)
      .map(v => (v._1, v._3, v._2, v._5)).persist(StorageLevel.MEMORY_AND_DISK)


    val realTimeWindow = realTimeData.window(Seconds(40)) //.cache()
    // ((extra_id,文字直播/视频直播/大直播),uid)
    val online = realTimeWindow.map(v => ((v._1, v._2), v._3))
      .window(Seconds(30))
      .filter(v => v._1._1 != "" && v._2 != "")
      .groupByKey()
      .map(v => ((v._1._1, v._1._2), v._2.toSet.size)) //(extra_id,文字直播/视频直播/大直播,在线人数)

    // ((extra_id,文字直播/视频直播/大直播,uid),c_time)
    //      val average = realTimeWindow.map(v => ((v._1, v._2, v._3), v._4))
    //        // ((extra_id,文字直播/视频直播/大直播,uid),(c_time,时长))
    //        .filter(v => v._1._1 != "" && v._1._3 != "")
    //        .updateStateByKey[(Long, Int)](updateFunc)
    //        .map(v => ((v._1._1, v._1._2), (1, v._2._2)))
    //        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    //        .map(v => ((v._1._1, v._1._2), v._2._2 / v._2._1))
    val average = data
      .filter(v => v._1 != "" && v._1 != null && v._1 != "null" && v._12 != "" && v._13 != "" && (v._3 == "圈子视频直播" || v._3 == "圈子文字直播" || v._3 == "大直播"))
      .map(v => ((v._1, v._3), v._12.toInt * 30 / v._13.toInt))

    val onlineResult = online.join(average).map(v => (
      v._1._1,
      if (v._1._2 == "圈子视频直播") 0 else if (v._1._2 == "圈子文字直播") 1 else if (v._1._2 == "大直播") 2 else 3,
      v._2._1,
      v._2._2,
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)))
    //          .map(v => Map("extra_id"-> v._1,"type"->v._2,"online"->v._3,"average"->v._4).toString())
    //        .map(v => "{" +
    //          "\"extra_id\":\"" + v._1+ "\"," +
    //          "\"type\"    :\"" + v._2+ "\"," +
    //          "\"online\"  :\"" + v._3+ "\"," +
    //          "\"average\" :\"" + v._4+ "\"," +
    //          "\"c_time\"  :\"" + v._5+ "\" " +
    //          "}")
    //      val map = Map("es.index.auto.create"->"true")
    //      map.+("es.nodes"->"127.0.0.1")
    //      map.+("es.resource.write"->"myindex/mytype")
    //      map.+("es.port"->"9200")
    //        EsSparkStreaming.saveJsonToEs(result,"/myindex/mytype")

    // 直播间圈子在线埋点数据入库
    realTimeData
      .map(v => (v._1, if (v._2 == "圈子视频直播") 0 else if (v._2 == "圈子文字直播") 1 else if (v._2 == "大直播") 2 else 3, v._3, v._4))
      .repartition(10)
      .foreachRDD(cs => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        try {
          Class.forName("com.mysql.jdbc.Driver").newInstance()
          cs.foreachPartition(f => {
            conn = DriverManager.getConnection("jdbc:mysql://192.168.195.212/finebi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "finebi", "@Lcs201707")
            //              conn = DriverManager.getConnection("jdbc:mysql://localhost/live_visit?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "root", "root")
            ps = conn.prepareStatement(" insert into lcs_real_time_count(extra_id,type,uid,c_time) values(?,?,?,?)")
            f.foreach(s => {
              ps.setString(1, s._1)
              ps.setInt(2, s._2)
              ps.setString(3, s._3)
              ps.setString(4, s._4)
              ps.executeUpdate()
            })
          })
        } catch {
          case t: Exception => t.printStackTrace() // TODO: handle error
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      })
    // 直播间在线数据入库
    onlineResult
      .foreachRDD(cs => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        try {
          Class.forName("com.mysql.jdbc.Driver").newInstance()
          cs.foreachPartition(f => {
            conn = DriverManager.getConnection("jdbc:mysql://192.168.195.212/finebi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "finebi", "@Lcs201707")
            //                  conn = DriverManager.getConnection("jdbc:mysql://localhost/live_visit?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "root", "root")
            ps = conn.prepareStatement(" insert into lcs_live_visit_online(extra_id,type,online,average,c_time) values(?,?,?,?,?)")
            f.foreach(s => {
              ps.setString(1, s._1)
              ps.setInt(2, s._2)
              ps.setInt(3, s._3)
              ps.setInt(4, s._4)
              ps.setString(5, s._5)
              ps.executeUpdate()
            })
          })
        } catch {
          case t: Exception => t.printStackTrace() // TODO: handle error
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      })

    // 直播间发言数据入库
    data
      //        .filter(v => v._6 == "app/balaComment")
      //        .map(v => {
      //          val dataMap = jsonParse(v._7)
      //          var dataJson = dataMap.getOrElse("data", "{}")
      //          if (null == dataJson || dataJson == "")
      //            dataJson = "{}"
      //          val cmn_info_map = jsonParse(dataJson)
      //          val cmn_info = cmn_info_map.getOrElse("cmn_info", "{}")
      //          val resultMap = jsonParse(cmn_info)
      //          (
      //            resultMap.getOrElse("uid", ""),
      //            resultMap.getOrElse("name", ""),
      //            resultMap.getOrElse("relation_id", ""),
      //            resultMap.getOrElse("content", ""),
      //            resultMap.getOrElse("c_time", "")
      //          )
      //    }).
      .filter(v => v._8 != "")
      .map(v => {
        (v._2, v._9, v._10, v._8, v._5, v._11) // (uid,name,relation_id,content,c_time,title)
      })
      .filter(_._5 != "")
      .repartition(10)
      .foreachRDD(cs => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        try {
          Class.forName("com.mysql.jdbc.Driver").newInstance()
          cs.foreachPartition(f => {
            conn = DriverManager.getConnection("jdbc:mysql://192.168.195.212/finebi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "finebi", "@Lcs201707")
            //                  conn = DriverManager.getConnection("jdbc:mysql://localhost/live_visit?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "root", "root")
            ps = conn.prepareStatement(" insert lcs_app_balacomment(uid,name,relation_id,content,c_time,title) values(?,?,?,?,?,?)")
            f.foreach(s => {
              val sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
              //    sdf1.setTimeZone(TimeZone.getTimeZone("UTC-0"))
              val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              ps.setString(1, s._1)
              ps.setString(2, s._2)
              //              ps.setString(2, new String(s._2.getBytes("UTF-8"),0,s._2.getBytes().length,"UTF-8"))
              ps.setString(3, s._3)
              ps.setString(4, s._4)
              //              ps.setString(4, new String(s._4.getBytes("UTF-8"),0,s._4.getBytes().length,"UTF-8"))
              ps.setString(5, sdf2.format(sdf1.parse(s._5)))
              ps.setString(6, s._6)
              try {
                ps.executeUpdate()
              } catch {
                case t: Exception => t.printStackTrace() // TODO: handle error
              }
            })
          })
        } catch {
          case t: Exception => t.printStackTrace() // TODO: handle error
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      })


    //启动
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true)
  }

  /**
   * 直播间平均在线时长统计，状态更新方法
   */
  val updateFunc = (currValues: Seq[String], prevValueState: Option[(Long, Int)]) => {
    //通过Spark内部的reduceByKey按key规约。然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    // 已累加的值

    val prevValue = prevValueState.getOrElse((0L, 0))
    var returnValue = prevValue
    // 来值不为空
    if (currValues.nonEmpty) {
      val currTime = currValues.max
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      var currTimestamp = 0L
      try {
        currTimestamp = sdf.parse(currTime).getTime
      } catch {
        case ex: ParseException => {
          ex.printStackTrace()
        }
      }
      // 前值为0（时间戳）
      if (returnValue._1 == 0L) {
        returnValue = (currTimestamp, returnValue._2)
      } else if (currTimestamp != 0L) {
        // 前值不为0（时间戳），且当前时间戳不为0
        val sec = currTimestamp - returnValue._1
        // 时间差相差31秒内
        // 来值与前值相差31秒内，认为正常的直播间状态上传，时长加上差值
        if (sec > 0 && sec <= 31000) {
          returnValue = (currTimestamp, sec.toInt / 1000 + returnValue._2)
        } else if (sec > 31) {
          // 来值与前值相差31秒已上，且
          returnValue = (currTimestamp, returnValue._2 + 15)
        }
      }
    } else {
      // 来值为空，且前值与当前时间戳比较大于35秒，认为离开直播间，时长加上默认值3秒
      if (returnValue._1 > 0 && (new Date().getTime - returnValue._1) > 35000) {
        returnValue = (0L, returnValue._2 + 15)
      }
    }


    Some(returnValue)
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

  def msTillTomorrow: Long = {
    val now = new Date()
    val tomorrow = new Date(now.getYear, now.getMonth, now.getDate + 1, 0, 0, 0)
    tomorrow.getTime - now.getTime
  }

  /**
   * 自定义数据源1：查询ES real_time_count/real_time_count表直播间在线数据 & 查询ES pro-yii2-elk-success/doc 接口数据：发言详情
   */
  class CustomReceiverFromEs() extends Receiver[String](StorageLevel.DISK_ONLY) {
    //启动的时候调用
    override def onStart(): Unit = {
      println("CustomReceiverFromEs启动了")
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
      sdf.setTimeZone(TimeZone.getTimeZone("UTC-0"))
      var preTime = sdf.format(new Date()) + "Z"
      while (!isStopped()) {
        val nowTime = sdf.format(new Date()) + "Z"
        Thread.sleep(5000)
        // 直播间实时数据
        val esData = esSearch(preTime, nowTime)
        // 发言数据
        val esData2 = esSearch2(preTime, nowTime)
        // 直播间今日聚合数据
        val esData3 = esSearch3()
        preTime = nowTime
        // 如果接收到了数据，就是用父类中的store方法进行保存
        if (esData != null && esData.nonEmpty)
          store(esData)
        if (esData2 != null && esData2.nonEmpty)
          store(esData2)
        if (esData3 != null && esData3.nonEmpty)
          store(esData3)
        //继续读取下一行数据
        Thread.sleep(5000)

      }
    }

    ////终止的时候调用
    override def onStop(): Unit = {
      println("CustomReceiverFromEs停止了")
    }

    /**
     * 查询ES数据real_time_count/real_time_count 直播间实时数据
     *
     * @param gte
     * @return
     */
    def esSearch(gte: String, lt: String): Iterator[String] = {
      val client = ESConfig.client()
      val boolBuilder = QueryBuilders.boolQuery()
      val sourceBuilder = new SearchSourceBuilder()
      //创建一个socket
      val rangeQueryBuilder = QueryBuilders.rangeQuery("logtime") //新建range条件
      //      rangeQueryBuilder.gte(gte) //开始时间
      rangeQueryBuilder.gte(gte); //开始时间
      rangeQueryBuilder.lt(lt); //结束时间
      boolBuilder.must(rangeQueryBuilder)

      val sortBuilder = SortBuilders.fieldSort("logtime").order(SortOrder.ASC) // 排训规则

      sourceBuilder.query(boolBuilder) //设置查询，可以是任何类型的QueryBuilder。
      sourceBuilder.from(0) //设置确定结果要从哪个索引开始搜索的from选项，默认为0
      sourceBuilder.size(10000) //设置确定搜素命中返回数的size选项，默认为10
      sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)) //设置一个可选的超时，控制允许搜索的时间。
      sourceBuilder.sort(sortBuilder)

      //sourceBuilder.fetchSource(new String[] {"fields.port","fields.entity_id","fields.message"}, new String[] {}) //第一个是获取字段，第二个是过滤的字段，默认获取全部
      val searchRequest = new SearchRequest("real_time_count") //索引
      searchRequest.types("real_time_count") //类型
      searchRequest.source(sourceBuilder)

      val searchHits: util.List[SearchHit] = ESConfig.scrollSearchAll(client, 10L, searchRequest)

      System.out.println("esSearch total hits : " + searchHits.size())
      client.close()
      import scala.collection.JavaConverters._
      searchHits.iterator().asScala.map(_.getSourceAsString)

    }

    /**
     * 查询ES数据pro-yii2-elk-success/doc 接口数据：发言详情
     *
     * @param gte
     * @param lt
     * @return
     */
    def esSearch2(gte: String, lt: String): Iterator[String] = {
      val client = ESConfig.client
      // bool查询
      val boolBuilder = QueryBuilders.boolQuery
      // bool子查询：范围查询
      //      val rangeQueryBuilder = QueryBuilders.rangeQuery("@timestamp") //新建range条件
      val rangeQueryBuilder = QueryBuilders.rangeQuery("c_time") //新建range条件
      rangeQueryBuilder.gte(gte); //开始时间
      rangeQueryBuilder.lt(lt); //结束时间
      boolBuilder.must(rangeQueryBuilder)
      // bool子查询：字符匹配查询
      //      val matchQueryBuilder = QueryBuilders.matchQuery("action", "app/balaComment") //这里可以根据字段进行搜索，must表示符合条件的，相反的mustnot表示不符合条件的
      //      boolBuilder.must(matchQueryBuilder)
      // 结果排序
      //        SortBuilder sortBuilder = SortBuilders.fieldSort("@timestamp").order(SortOrder.ASC);// 排训规则
      // 新建查询源
      val sourceBuilder = new SearchSourceBuilder
      sourceBuilder.query(boolBuilder) //设置查询，可以是任何类型的QueryBuilder。
      sourceBuilder.from(0) //设置确定结果要从哪个索引开始搜索的from选项，默认为0
      sourceBuilder.size(10000) //设置确定搜素命中返回数的size选项，默认为10
      sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)) //设置一个可选的超时，控制允许搜索的时间。

      //        sourceBuilder.sort(sortBuilder);
      // 第一个是获取字段，第二个是过滤的字段，默认获取全部
      //sourceBuilder.fetchSource(new String[] {"fields.port","fields.entity_id","fields.message"}, new String[] {});
      /**
       * 制定索引库和类型表
       */
      //        SearchRequest searchRequest = new SearchRequest("real_time_count"); //索引：实时库
      //        searchRequest.types("real_time_count"); //类型:实时表

      val searchRequest = new SearchRequest("checkcirclecomment") //索引：接口库——success
      searchRequest.types("checkcirclecomment") //类型:接口请求记录
      //      val searchRequest = new SearchRequest("pro-yii2-elk-success") //索引：接口库——success
      //      searchRequest.types("doc") //类型:接口请求记录

      searchRequest.source(sourceBuilder)
      // 深度查询：分页查询全量数据
      val searchHits = ESConfig.scrollSearchAll(client, 10L, searchRequest)
      System.out.println("esSearch2 total hits : " + searchHits.size())
      client.close()
      import scala.collection.JavaConversions._
      searchHits.iterator().map(_.getSourceAsString)

    }

    /**
     * 查询ES数据real_time_count/real_time_count 直播间数据
     *
     * @return
     */
    def esSearch3(): Iterator[String] = {
      val client = ESConfig.client
      // bool查询
      val boolBuilder = QueryBuilders.boolQuery
      // bool子查询：范围查询
      val rangeQueryBuilder = QueryBuilders.rangeQuery("logtime") //新建range条件
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      rangeQueryBuilder.gte(sdf.format(new Date) + "T00:00:00+08:00") //开始时间
      rangeQueryBuilder.lt("now") //结束时间
      boolBuilder.must(rangeQueryBuilder)
      // bool子查询：字符匹配查询
      boolBuilder.must(QueryBuilders.matchQuery("is_online", "1"))
      boolBuilder.should(QueryBuilders.matchQuery("type", "圈子视频直播"))
      boolBuilder.should(QueryBuilders.matchQuery("type", "圈子文字直播"))
      boolBuilder.minimumShouldMatch(1)
      // 结果排序
      //        SortBuilder sortBuilder = SortBuilders.fieldSort("@timestamp").order(SortOrder.ASC);// 排训规则
      // 新建查询源
      val sourceBuilder = new SearchSourceBuilder
      sourceBuilder.query(boolBuilder) //设置查询，可以是任何类型的QueryBuilder。
      sourceBuilder.from(0) //设置确定结果要从哪个索引开始搜索的from选项，默认为0
      sourceBuilder.size(10000) //设置确定搜素命中返回数的size选项，默认为10
      sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)) //设置一个可选的超时，控制允许搜索的时间。
      // 聚合操作
      val aggs1 = AggregationBuilders.terms("extra_id_dist").field("extra_id").size(10000)
      val aggs2 = AggregationBuilders.terms("type_count").field("type")
      val aggs3 = AggregationBuilders.cardinality("device_id_count").field("device_id")
      //        aggs1.subAggregation(aggs2);
      aggs1.subAggregation(aggs2.subAggregation(aggs3))
      sourceBuilder.aggregation(aggs1)
      //        sourceBuilder.sort(sortBuilder);
      // 第一个是获取字段，第二个是过滤的字段，默认获取全部
      //sourceBuilder.fetchSource(new String[] {"fields.port","fields.entity_id","fields.message"}, new String[] {});
      /**
       * 制定索引库和类型表
       */
      val searchRequest = new SearchRequest("real_time_count") //索引：接口库——success
      searchRequest.types("real_time_count") //类型:接口请求记录

      searchRequest.source(sourceBuilder)
      // 深度查询：分页查询全量数据
      //        List<SearchHit> searchHits = ESConfig.scrollSearchAll(client, 10L, searchRequest);
      //        for (SearchHit hit : searchHits) {
      //            System.out.println("search -> " + hit.getSourceAsString());
      //        }
      //        System.out.println(searchHits.size());
      val searchResponse = client.search(searchRequest)

      val terms1: Terms = searchResponse.getAggregations.get("extra_id_dist")
      val result = new util.ArrayList[String]
      System.out.println("esSearch3 total hits : " + terms1.getBuckets.size)
      import scala.collection.JavaConversions._
      for (bucket1 <- terms1.getBuckets) {
        val terms2: Terms = bucket1.getAggregations.get("type_count")
        import scala.collection.JavaConversions._
        for (bucket2 <- terms2.getBuckets) {
          val jsonObj = new JSONObject
          //                System.out.println("key:" + bucket1.getKey().toString() + "###" + bucket2.getKey().toString() + ";value:" + bucket2.getDocCount());
          val deviceIdCount: Cardinality = bucket2.getAggregations.get("device_id_count")
          //                System.out.println("key:" + bucket1.getKey().toString() + "###" + deviceIdCount.getName()+ ";value:" + deviceIdCount.getValue());
          jsonObj.put("extra_id", bucket1.getKey.toString)
          jsonObj.put("type", bucket2.getKey.toString)
          jsonObj.put("points", bucket2.getDocCount.toString)
          jsonObj.put("device_id_count", deviceIdCount.getValue.toString)
          result.add(jsonObj.toJSONString)
        }
      }
      import scala.collection.JavaConversions._

      client.close()
      result.toIterator
    }
  }

}
