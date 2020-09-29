package cn.yintech.eventLog

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import java.util.concurrent.TimeUnit

import cn.yintech.esUtil.ESConfig
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.sql.SparkSession
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object SparkReadEsRealTimeCountOld {
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
      .appName("SparkReadEsRealTimeCount")
      .config("hive.exec.dynamic.partition", "true")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val lineDataSet = spark.read.format("jdbc")
//            .option("url", "jdbc:mysql://rm-2zebtm824um01072v5o.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false")
      .option("url", "jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "licaishi_w")
      .option("password", "a222541420a50a5")
      .option("dbtable", "lcs_circle_notice")
      .load()
    lineDataSet.createOrReplaceTempView("lcs_circle_notice")
    import spark.implicits._
    getBetweenDates(startDt,endDt).foreach( dt => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")

      val cal1 = Calendar.getInstance()
      cal1.setTime(sdf.parse(dt))
      cal1.add(Calendar.DAY_OF_YEAR,1)
      val dt_add1 = sdf.format(cal1.getTime)

      val cal2 = Calendar.getInstance()
      cal2.setTime(sdf.parse(dt))
      cal2.add(Calendar.DAY_OF_YEAR,-1)
      val dt_sub1 = sdf.format(cal2.getTime)

      spark.sql(s"select id,circle_id,u_type,uid,title,cast(start_time as String) start_time,cast(end_time as String) end_time , live_status from lcs_circle_notice where start_time between '$dt_sub1' and '$dt_add1' and live_status = 0 order by id")
        .map(row => (row.getLong(0).toString, row.getLong(1).toString, row.getInt(2).toString, row.getDecimal(3).toString, row.getString(4), row.getString(5), row.getString(6), row.getInt(7).toString))
        .flatMap(v => {
          esSearch(v._2,v._6,v._7).map( a => {
            val jsonObj = jsonParse(a)
            (
              v._1,
              v._6,
              v._7,
              jsonObj.getOrElse("extra_id",""),
              jsonObj.getOrElse("device_id",""),
              jsonObj.getOrElse("uid",""),
              jsonObj.getOrElse("ext",""),
              jsonObj.getOrElse("fr",""),
              jsonObj.getOrElse("is_online",""),
              jsonObj.getOrElse("p_uid",""),
              jsonObj.getOrElse("c_time",""),
              "圈子视频直播"
            )
          })
        })
        .toDF("live_id","start_time","end_time","extra_id","device_id","uid","ext","fr","is_online","p_uid","c_time","type")
        .coalesce(1)
        .createOrReplaceTempView("table1")
      spark.sql(s"insert OVERWRITE table ods.ods_es_real_time_count_old_1d partition(dt='$dt') select * from table1 WHERE c_time between '$dt' and '$dt_add1'")
      spark.sql(s"insert OVERWRITE table ods.ods_es_real_time_count_old_1d partition(dt='$dt_add1') select * from table1 WHERE c_time > '$dt_add1'")
    })

    spark.stop()
  }

  def esSearch(extraId : String , startTime: String, endTime: String): List[String] = {
    val client = ESConfig.client()
    val boolBuilder = QueryBuilders.boolQuery()
    val sourceBuilder = new SearchSourceBuilder()
    //创建一个socket
    val rangeQueryBuilder = QueryBuilders.rangeQuery("logtime") //新建range条件
    if (startTime.contains("now")) rangeQueryBuilder.gte(startTime)
    else rangeQueryBuilder.gte(startTime.replace(" ", "T") + "+08:00") //开始时间
    if (endTime.contains("now")) rangeQueryBuilder.lte(endTime)
    else rangeQueryBuilder.lte(endTime.replace(" ", "T") + "+08:00") //结束时间
    boolBuilder.must(rangeQueryBuilder)
    if (extraId.length > 0) boolBuilder.must(QueryBuilders.matchQuery("extra_id", extraId))
    boolBuilder.must(rangeQueryBuilder)
    boolBuilder.must(QueryBuilders.matchQuery("type", "圈子视频直播"))

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
