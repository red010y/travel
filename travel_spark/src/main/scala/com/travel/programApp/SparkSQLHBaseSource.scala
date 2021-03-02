package com.travel.programApp

import com.travel.transaction.{DriverTransation, HotAreaOrder, HotOrderTransation, OrderTransation, RenterTransation}
import com.travel.utils.GlobalConfigUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLHBaseSource {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("sparkSQLHBase")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val order: DataFrame = sparkSession.read
      .format("com.travel.programApp.hbaseSource.HBaseSource")
      .options(Map(
//        哪里获取？在application.conf中
//        sparkSQL查询表schema=》cc：StructType
        GlobalConfigUtils.getProp("sparksql_table_schema") -> GlobalConfigUtils.getProp("order.sparksql_table_schema"),
//        hbase表名
        GlobalConfigUtils.getProp("hbase_table_name") -> GlobalConfigUtils.getProp("syn.table.order_info"),
//        hbase查询表的cf：cc
        GlobalConfigUtils.getProp("hbase_table_schema") -> GlobalConfigUtils.getProp("order.hbase_table_schema")
      )).load()

    val driver: DataFrame = sparkSession.read
      .format("com.travel.programApp.hbaseSource.HBaseSource")
      .options(Map(
        GlobalConfigUtils.getProp("sparksql_table_schema") -> GlobalConfigUtils.getProp("drivers.spark_sql_table_schema"),
        GlobalConfigUtils.getProp("hbase_table_name") -> GlobalConfigUtils.getProp("syn.table.driver_info"),
        GlobalConfigUtils.getProp("hbase_table_schema") -> GlobalConfigUtils.getProp("driver.hbase_table_schema")
      )).load()

    val renter: DataFrame = sparkSession.read
      .format("com.travel.programApp.hbaseSource.HBaseSource")
      .options(Map(
        GlobalConfigUtils.getProp("sparksql_table_schema") -> GlobalConfigUtils.getProp("registe.sparksql_table_schema"),
        GlobalConfigUtils.getProp("hbase_table_name") -> GlobalConfigUtils.getProp("syn.table.renter_info"),
        GlobalConfigUtils.getProp("hbase_table_schema") -> GlobalConfigUtils.getProp("registe.hbase_table_schema")
      )).load()

    //注册
    order.createOrReplaceTempView("order")
    driver.createOrReplaceTempView("driver")
    renter.createOrReplaceTempView("renter")
    //cache
    sparkSession.sqlContext.cacheTable("order")
    sparkSession.sqlContext.cacheTable("driver")
    sparkSession.sqlContext.cacheTable("renter")


//    订单监控
//    最后将查出来的数据写入到hbase(数据量大,hbase查询效率高)
    OrderTransation.init(sparkSession)
//
//    用户数据
    RenterTransation.init(sparkSession)
//
//    司机表
    DriverTransation.init(sparkSession)
//
//    统计热门订单到hbase表中
    HotOrderTransation.init(sparkSession)
//    热门街道订单统计
    HotAreaOrder.init(sparkSession)

  }
}

