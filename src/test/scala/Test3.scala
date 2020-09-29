import cn.yintech.esUtil.ESConfig
import cn.yintech.hbaseUtil.HbaseUtils
import cn.yintech.hbaseUtil.HbaseUtils.getRow
import cn.yintech.online.LiveVisitOnline.jsonParse
import org.apache.hadoop.hbase.TableName


object Test3 {
  def main(args: Array[String]): Unit = {
    import scala.collection.JavaConversions._
    val onlineFromEs = ESConfig.searchOnlineAgg("61292", "2020-08-28 11:20:34", "2020-08-28 12:20:00")
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
    println("onlineFromEsList" + onlineFromEsList.map(_._5).distinct.size)
    val deviceidUseridMap = Map(onlineFromEsList.map(v => (v._5,v._3)).sortBy(_._2.length):_*)
    var num = 0
    deviceidUseridMap
      .foreach( v  => {
        if (v._2.length > 0){
          num +=1
        } else if (v._1.length > 0){
          num +=1
        }
      })


    println("map:"+deviceidUseridMap.size)
    println("num:"+num)


//    val conn = HbaseUtils.getConnection
//    val table2 = TableName.valueOf("lcs_live_visit_new_user")
//    val htable2 = conn.getTable(table2)
//
//    val hbaseAllNewUsers = getRow(htable2, "62922".reverse)
//    val allNewUsers = if (hbaseAllNewUsers.nonEmpty) hbaseAllNewUsers.get(0) else "0"
//
//    println("allNewUsers" + allNewUsers)
//
//    // 老用户
//    val oldUser = onlineFromEsList.filter(v => {
//      println(v._3,!allNewUsers.contains(v._3) || v._3 == "")
//      !allNewUsers.contains(v._3) || v._3 == ""
//    })
//    // 新用户
//    val newUser = onlineFromEsList.filter(v => allNewUsers.contains(v._3) &&  v._3.length > 0)
//
//    println("newUser:" + newUser.map(_._3).distinct.size)
//    println("oldUser:" + oldUser.size)




  }
}
