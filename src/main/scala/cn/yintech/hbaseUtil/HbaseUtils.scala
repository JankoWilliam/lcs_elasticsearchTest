package cn.yintech.hbaseUtil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.util.Bytes

object HbaseUtils {
  def main(args: Array[String]): Unit = {
    val conn = ConnectionFactory.createConnection(getHbaseConf)
    val table = TableName.valueOf("user_live_visit_lcs")
    val htable = conn.getTable(table)
    getRow(htable,"99638962")

  }

  def getConnection : Connection = {
    ConnectionFactory.createConnection(getHbaseConf)
  }

  def getHbaseConf: Configuration = {
    val conf: Configuration = HBaseConfiguration.create
//    conf.addResource(".\\main\\resources\\hbase-site.xml")
    conf.set("hbase.zookeeper.property.clientPort","2181")
    conf.set("hbase.zookeeper.quorum","bigdata003.sj.com")
//    conf.set("hbase.zookeeper.quorum","bigdata002,bigdata003,bigdata004")
    /*conf.set("spark.executor.memory","3000m")
    conf.set("hbase.master","master:60000")
    conf.set("hbase.rootdir","Contant.HBASE_ROOTDIR")*/
    conf
  }

  //获取数据
  def getRow(table: Table, rowKey: String): List[String] = {
    val get: Get = new Get(Bytes.toBytes(rowKey))//.addFamily(Bytes.toBytes("cf1"))
    val resultSet: Result = table.get(get)
    var result = List[String]()
    for (rowKv <- resultSet.rawCells()) {
//      println("Famiily:" + new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength, "UTF-8"))
//      println("Qualifier:" + new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8"))
//      println("TimeStamp:" + rowKv.getTimestamp)
//      println("rowkey:" + new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8"))
//      println("Value:" + new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8"))
      result = result.::(new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8"))
    }
    result
  }
  def setRow(table: Table, row: String, columnFaily: String, column: String, value: String): Unit = {
    val put: Put = new Put(Bytes.toBytes(row))
    put.addColumn(Bytes.toBytes(columnFaily), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
  }
}
