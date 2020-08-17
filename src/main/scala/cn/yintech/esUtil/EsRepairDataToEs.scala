package cn.yintech.esUtil

import java.util
import java.util.concurrent.TimeUnit

import cn.yintech.online.LiveVisitOnline.jsonParse
import net.minidev.json.JSONObject
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}

/**
 * ES集群之间修补数据——20200810
 */
object EsRepairDataToEs {
  def main(args: Array[String]): Unit = {
    //    val client = ESConfig.client()
    //        val lcsTestClient = ESConfigLcsTest.client()
    //        println(esSearchPro_yii2_elk_success( "2020-08-07 16:24:01", "2020-08-07 16:54:01").size)
    //    esSearchPro_yii2_elk_success( "2020-08-07 16:24:01", "2020-08-07 16:54:01").foreach(println(_))

    // 批量导入数据到ES
    val request = new BulkRequest

//    Array(("2020-08-07 16:24:01", "2020-08-07 16:30:00"),
//      ("2020-08-07 16:30:01", "2020-08-07 16:40:00"),
//      ("2020-08-07 16:40:01", "2020-08-07 16:50:00"),
//      ("2020-08-07 16:50:01", "2020-08-07 17:00:00")
//    )

//      Array(
//        ("2020-08-07 17:00:01", "2020-08-07 17:30:00"),
//        ("2020-08-07 17:30:01", "2020-08-07 18:00:00"),
//        ("2020-08-07 18:00:01", "2020-08-07 18:30:00")
//      )

//    Array(
//      ("2020-08-07 18:30:01", "2020-08-07 19:00:00"),
//      ("2020-08-07 19:00:01", "2020-08-07 19:30:00"),
//      ("2020-08-07 19:30:01", "2020-08-07 19:40:00"),
//      ("2020-08-07 19:40:01", "2020-08-07 19:50:00")
//    )


//    Array(
//      ("2020-08-07 19:50:01", "2020-08-07 20:00:00"),
//      ("2020-08-07 20:00:01", "2020-08-07 20:10:00"),
//      ("2020-08-07 20:10:01", "2020-08-07 20:20:00"),
//      ("2020-08-07 20:20:01", "2020-08-07 20:30:00")
//    )

//    Array(
//      ("2020-08-07 20:30:01", "2020-08-07 20:40:00"),
//      ("2020-08-07 20:40:01", "2020-08-07 20:50:00"),
//      ("2020-08-07 20:50:01", "2020-08-07 21:00:00")
//    )

//    Array(
//      ("2020-08-07 21:00:01", "2020-08-07 21:10:00"),
//      ("2020-08-07 21:10:01", "2020-08-07 21:20:00"),
//      ("2020-08-07 21:20:01", "2020-08-07 21:30:00")
//    )

//    Array(
//      ("2020-08-07 21:30:01", "2020-08-07 21:40:00"),
//      ("2020-08-07 21:40:01", "2020-08-07 21:50:00"),
//      ("2020-08-07 21:50:01", "2020-08-07 22:00:00")
//    )

//    Array(
//      ("2020-08-07 22:00:01", "2020-08-07 22:10:00"),
//      ("2020-08-07 22:10:01", "2020-08-07 22:20:00"),
//      ("2020-08-07 22:20:01", "2020-08-07 22:30:00")
//    )

//    Array(
//      ("2020-08-07 22:30:01", "2020-08-07 22:40:00"),
//      ("2020-08-07 22:40:01", "2020-08-07 22:50:00"),
//      ("2020-08-07 22:50:01", "2020-08-07 23:00:00")
//)

    Array(("2020-08-07 23:00:01", "2020-08-07 23:10:00"),
      ("2020-08-07 23:10:01", "2020-08-07 23:20:00"),
      ("2020-08-07 23:20:01", "2020-08-07 23:30:00"),
      ("2020-08-07 23:30:01", "2020-08-07 23:54:01"))

      .foreach(t => {
        esSearchPro_yii2_elk_success(t._1, t._2).foreach(v => {
          val indexRequest = new IndexRequest("real_time_count", "real_time_count")
          val dataMap = jsonParse(v)
          val uid = dataMap.getOrElse("uid", "")
          val ip = dataMap.getOrElse("client_ip", "")
          val params = dataMap.getOrElse("params", "{}")
          val device_id = dataMap.getOrElse("deviceId", "")
          val c_time = dataMap.getOrElse("log_time", "")
          val logtime = c_time.replace(" ", "T") + "+08:00"

          val dataMap2 = jsonParse(params)
          val get = dataMap2.getOrElse("get", "")
          val header = dataMap2.getOrElse("header", "")

          val dataMap3 = jsonParse(get)
          val fr = dataMap3.getOrElse("fr", "")
          val `type` = dataMap3.getOrElse("type", "")
          val extra_id = dataMap3.getOrElse("extra_id", "")
          val source = dataMap3.getOrElse("source", "")
          val is_small_window = dataMap3.getOrElse("is_small_window", "")
          val ext = "{\"is_small_window\":\"" + is_small_window + "\"}"
          val is_online = dataMap3.getOrElse("is_online", "")
          val p_uid = dataMap3.getOrElse("p_uid", "")

          val dataMap4 = jsonParse(header)
          val refer = dataMap4.getOrElse("referer", "")

          val u_type = new Integer(1)


          val jsonObj = new JSONObject

          jsonObj.put("type", `type`)
          jsonObj.put("extra_id", extra_id)
          jsonObj.put("is_online", is_online)
          jsonObj.put("fr", fr)
          jsonObj.put("uid", uid)
          jsonObj.put("u_type", u_type)
          jsonObj.put("p_uid", p_uid)
          jsonObj.put("refer", refer)
          jsonObj.put("device_id", device_id)
          jsonObj.put("c_time", c_time)
          jsonObj.put("ip", ip)
          jsonObj.put("logtime", logtime)
          jsonObj.put("ext", ext)
          jsonObj.put("source", source)

          jsonObj.put("is_insert", "1")


          indexRequest.source(jsonObj.toJSONString(), XContentType.JSON)
          request.add(indexRequest)
        })
        println("done " + t)
      })
    val testClient = ESConfig.client()
    testClient.bulk(request)
    testClient.close()


  }


  def esSearch(client: RestHighLevelClient, startTime: String, endTime: String): List[String] = {
    //    val client = ESConfig.client()
    val boolBuilder = QueryBuilders.boolQuery()
    val sourceBuilder = new SearchSourceBuilder()
    //创建一个socket
    val rangeQueryBuilder = QueryBuilders.rangeQuery("logtime") //新建range条件
    if (startTime.contains("now")) rangeQueryBuilder.gte(startTime)
    else rangeQueryBuilder.gte(startTime.replace(" ", "T") + "+08:00") //开始时间
    if (endTime.contains("now")) rangeQueryBuilder.lte(endTime)
    else rangeQueryBuilder.lte(endTime.replace(" ", "T") + "+08:00") //结束时间
    boolBuilder.must(rangeQueryBuilder)
    //    if (extraId.length > 0) boolBuilder.must(QueryBuilders.matchQuery("extra_id", extraId))
    boolBuilder.must(rangeQueryBuilder)
    //    boolBuilder.must(QueryBuilders.matchQuery("type", "圈子视频直播"))

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
    searchHits.asScala.map(_.getSourceAsString).toList

  }

  def esSearchPro_yii2_elk_success(startTime: String, endTime: String): List[String] = {
    val client = ESConfig.client()
    val boolBuilder = QueryBuilders.boolQuery()
    val sourceBuilder = new SearchSourceBuilder()
    //创建一个socket
    val rangeQueryBuilder = QueryBuilders.rangeQuery("log_time.keyword") //新建range条件
    rangeQueryBuilder.format("yyyy-MM-dd hh:mm:ss")
    rangeQueryBuilder.gte(startTime)
    rangeQueryBuilder.lte(endTime)
    boolBuilder.must(rangeQueryBuilder)
    boolBuilder.must(QueryBuilders.matchQuery("action.keyword", "app/realTimeCount"))
    //    boolBuilder.must(QueryBuilders.nestedQuery("data",QueryBuilders.matchQuery("data.code", "0"))

    //    val sortBuilder = SortBuilders.fieldSort("logtime").order(SortOrder.ASC) // 排训规则

    sourceBuilder.query(boolBuilder) //设置查询，可以是任何类型的QueryBuilder。
    sourceBuilder.from(0) //设置确定结果要从哪个索引开始搜索的from选项，默认为0
    sourceBuilder.size(10000) //设置确定搜素命中返回数的size选项，默认为10
    sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)) //设置一个可选的超时，控制允许搜索的时间。
    //    sourceBuilder.sort(sortBuilder)

    sourceBuilder.fetchSource(Array[String]("log_time", "deviceId", "action", "data", "params", "uid", "client_ip"), Array[String]()) //第一个是获取字段，第二个是过滤的字段，默认获取全部
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
}
