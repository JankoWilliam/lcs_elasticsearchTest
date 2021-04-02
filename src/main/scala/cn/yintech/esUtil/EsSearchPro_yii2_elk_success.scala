package cn.yintech.esUtil

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import cn.yintech.eventLog.SparkReadEsRealTimeCount.jsonParse
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

object EsSearchPro_yii2_elk_success {

  def main(args: Array[String]): Unit = {

    getViewDetailCountWriterTxt()
    newInfoCirclePuidCountWriterTxt()

  }

  def newInfoCirclePuidCountWriterTxt() = {

    val writer = new PrintWriter(new File("C:\\Users\\zhanpeng.fu\\Desktop\\理财师主页访问20210201_20210317.txt"))
    writer.println("date\tpuid\tuv\tpv")
    getBetweenDates("2021-02-01", "2021-03-17").foreach(d => {
      println("date:" + d)
      val strings: immutable.Seq[String] = newInfoCirclePuidCount(s"$d 00:00:00", s"$d 08:00:00")
        .union(newInfoCirclePuidCount(s"$d 08:00:00", s"$d 10:00:00"))
        .union(newInfoCirclePuidCount(s"$d 10:00:00", s"$d 12:00:00"))
        .union(newInfoCirclePuidCount(s"$d 12:00:00", s"$d 14:00:00"))
        .union(newInfoCirclePuidCount(s"$d 14:00:00", s"$d 16:00:00"))
        .union(newInfoCirclePuidCount(s"$d 16:00:00", s"$d 18:00:00"))
        .union(newInfoCirclePuidCount(s"$d 18:00:00", s"$d 20:00:00"))
        .union(newInfoCirclePuidCount(s"$d 20:00:00", s"$d 22:00:00"))
        .union(newInfoCirclePuidCount(s"$d 22:00:00", s"$d 24:00:00"))

      strings.map(v => {
        val json = jsonParse(v)
        val dataStr = json.getOrElse("data", "{}")
        val dataJson = jsonParse(dataStr)
        val data2Str = dataJson.getOrElse("data", "{}")
        val data2Json = jsonParse(data2Str)
        val plannerStr = data2Json.getOrElse("planner", "{}")
        val plannerJson = jsonParse(plannerStr)
        val planner_infoStr = plannerJson.getOrElse("planner_info", "{}")
        val planner_infoJson = jsonParse(planner_infoStr)
        val p_uid = planner_infoJson.getOrElse("p_uid", "")

        val deviceId = json.getOrElse("deviceId", "")
        (p_uid, deviceId)

      }).groupBy(_._1).map(v => {
        val pv = v._2.size
        val uv = v._2.map(_._2).distinct.size
        (v._1, uv, pv)
      }).foreach(v => writer.println(d + "\t" + v._1 + "\t" + v._2 + "\t" + v._3))
    })
    writer.close()

  }

  def dynamicDetailCountWriterTxt() = {
    val writer = new PrintWriter(new File("C:\\Users\\zhanpeng.fu\\Desktop\\123.txt"))
    writer.println("date\tdynamic_id\tcount")
    getBetweenDates("2021-03-01", "2021-03-17").foreach(v => {
      val strings: immutable.Seq[String] = fortuneCircleDynamicDetailCount(v + " 00:00:00", v + " 24:00:00")
      strings.map(v => {
        val jsonData = jsonParse(v)
        val paramsStr = jsonData.getOrElse("params", "{}")
        val logTime = jsonData.getOrElse("log_time", "")
        val paramsJson = jsonParse(paramsStr)
        val postStr = paramsJson.getOrElse("post", "{}")
        val postJson = jsonParse(postStr)
        val dynamic_id = postJson.getOrElse("dynamic_id", "")
        (logTime.substring(0, 10), dynamic_id)
      }).groupBy(v => (v._1, v._2)).map(v => (v._1._1, v._1._2, v._2.size))
        .foreach(v => writer.println((v._1 + "\t" + v._2 + "\t" + v._3)))
    })
    writer.close()
  }

  def getViewDetailCountWriterTxt() = {
    val writer = new PrintWriter(new File("C:\\Users\\zhanpeng.fu\\Desktop\\观点详情页20210201_20210317.txt"))
    writer.println("date\tv_id\tuv\tpv")
    getBetweenDates("2021-02-01", "2021-03-17").foreach(d => {
      println("date:" + d)
      val strings: immutable.Seq[String] = getViewDetailCount(d + " 00:00:00", d + " 24:00:00")
      strings.map(v => {
        val json = jsonParse(v)
        val paramsStr = json.getOrElse("params", "{}")
        val paramsJson = jsonParse(paramsStr)
        val getStr = paramsJson.getOrElse("get", "{}")
        val getJson = jsonParse(getStr)
        val v_id = getJson.getOrElse("v_id", "")

        val deviceId = json.getOrElse("deviceId", "")
        (v_id, deviceId)
      }).groupBy(_._1).map(v => {
        val pv = v._2.size
        val uv = v._2.map(_._2).distinct.size
        (v._1, uv, pv)
      }).foreach(v => writer.println(d + "\t" + v._1 + "\t" + v._2 + "\t" + v._3))
    })
    writer.close()
  }

  def fortuneCircleDynamicDetailCount(startTime: String, endTime: String): List[String] = {
    val client = ESConfig.client()
    val boolBuilder = QueryBuilders.boolQuery()
    val sourceBuilder = new SearchSourceBuilder()
    //创建一个socket
    val rangeQueryBuilder = QueryBuilders.rangeQuery("log_time.keyword") //新建range条件
    rangeQueryBuilder.gte(startTime)
    rangeQueryBuilder.lt(endTime)
    boolBuilder.must(rangeQueryBuilder)
    boolBuilder.must(QueryBuilders.matchQuery("action.keyword", "api/fortuneCircleDynamicDetail"))
    boolBuilder.must(QueryBuilders.matchQuery("params", "dynamic_id"))

    //    boolBuilder.must(QueryBuilders.nestedQuery("data",QueryBuilders.matchQuery("data.code", "0"))
    //    val sortBuilder = SortBuilders.fieldSort("logtime").order(SortOrder.ASC) // 排训规则

    sourceBuilder.query(boolBuilder) //设置查询，可以是任何类型的QueryBuilder。
    sourceBuilder.from(0) //设置确定结果要从哪个索引开始搜索的from选项，默认为0
    sourceBuilder.size(10000) //设置确定搜素命中返回数的size选项，默认为10
    sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)) //设置一个可选的超时，控制允许搜索的时间。
    //    sourceBuilder.sort(sortBuilder)

    sourceBuilder.fetchSource(Array[String]("log_time", "deviceId", "action", "data", "params"), Array[String]()) //第一个是获取字段，第二个是过滤的字段，默认获取全部
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

  def newInfoCirclePuidCount(startTime: String, endTime: String): List[String] = {
    val client = ESConfig.client()
    val boolBuilder = QueryBuilders.boolQuery()
    val sourceBuilder = new SearchSourceBuilder()
    //创建一个socket
    val rangeQueryBuilder = QueryBuilders.rangeQuery("log_time.keyword") //新建range条件
    rangeQueryBuilder.gte(startTime)
    rangeQueryBuilder.lt(endTime)
    boolBuilder.must(rangeQueryBuilder)
    boolBuilder.must(QueryBuilders.matchQuery("action.keyword", "app/newInfoCircle"))
    boolBuilder.must(QueryBuilders.matchQuery("data", "p_uid"))

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

  def getViewDetailCount(startTime: String, endTime: String): List[String] = {
    val client = ESConfig.client()
    val boolBuilder = QueryBuilders.boolQuery()
    val sourceBuilder = new SearchSourceBuilder()
    //创建一个socket
    val rangeQueryBuilder = QueryBuilders.rangeQuery("log_time.keyword") //新建range条件
    rangeQueryBuilder.gte(startTime)
    rangeQueryBuilder.lt(endTime)
    boolBuilder.must(rangeQueryBuilder)
    boolBuilder.must(QueryBuilders.matchQuery("action.keyword", "api/getViewDetail"))
    boolBuilder.must(QueryBuilders.matchQuery("params", "v_id"))

    //    boolBuilder.must(QueryBuilders.nestedQuery("data",QueryBuilders.matchQuery("data.code", "0"))
    //    val sortBuilder = SortBuilders.fieldSort("logtime").order(SortOrder.ASC) // 排训规则

    sourceBuilder.query(boolBuilder) //设置查询，可以是任何类型的QueryBuilder。
    sourceBuilder.from(0) //设置确定结果要从哪个索引开始搜索的from选项，默认为0
    sourceBuilder.size(10000) //设置确定搜素命中返回数的size选项，默认为10
    sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)) //设置一个可选的超时，控制允许搜索的时间。
    //    sourceBuilder.sort(sortBuilder)

    sourceBuilder.fetchSource(Array[String]("log_time", "deviceId", "action","params"), Array[String]()) //第一个是获取字段，第二个是过滤的字段，默认获取全部
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
}
