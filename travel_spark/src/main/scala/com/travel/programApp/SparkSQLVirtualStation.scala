package com.travel.programApp

import java.util

import com.travel.common.{Constants, District}
import com.travel.utils.{HbaseTools, SparkUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.{Point, Polygon}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable

object SparkSQLVirtualStation {
  def main(args: Array[String]): Unit = {

      val sparkConf: SparkConf = new SparkConf().setAppName("StreamingKafka").setMaster("local[2]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")

    //TODO 第一步：从hbase中获取海口市的数据
    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "node1,node2,node3")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    //设置hbase连接超时时间
    hconf.setInt("hbase.client.operation.timeout", 3000)

    //读取hbase的数据，获取到了df
    //读取hase的方法：newAPIHadoopRDD（）返回RDD,loadHBaseData内部就调用了这个接口
    val hbaseFrame: DataFrame = HbaseTools.loadHBaseData(sparkSession,hconf)
    hbaseFrame.createOrReplaceTempView("order_df")


    //TODO 第二步：计算出所有的虚拟车站
    val virtual_rdd:RDD[Row] = SparkUtils.getVirtualFrame(sparkSession)

//TODO 第三步：确定海口市每个区的边界
//    得到海口市每个区的边界形状,并且将边界进行广播
    val districtBroadCast: Broadcast[util.ArrayList[District]] = SparkUtils.broadCastDistrictValue(sparkSession)

    //TODO 第四步：判断虚拟车站属于哪个区，接口
    //  判断每个虚拟车站一个经纬度坐落在哪一个区里面，就能知道每个区里面有多少个虚拟车站了
    val finalSaveRow: RDD[mutable.Buffer[Row]] = virtual_rdd.mapPartitions(eachParrtition => {
      //使用JTS-Tools来通过多个经纬度，画出多边形
      import org.geotools.geometry.jts.JTSFactoryFinder
      val geometryFactory = JTSFactoryFinder.getGeometryFactory(null)
      var reader = new WKTReader(geometryFactory)
      //将每一个区的，每一个边界求出来
      //将我们每一个区的经纬度的点连接起来，成为一个形状
      val wktPolygons: mutable.Buffer[(District, Polygon)] = SparkUtils.changeDistictToPolygon(districtBroadCast, reader)

      //每个虚拟车站的数据
      eachParrtition.map(row => {
        //判断虚拟车站在哪个区域
        val lng = row.getAs[String]("starting_lng")
        val lat = row.getAs[String]("starting_lat")
        val wktPoint = "POINT(" + lng + " " + lat + ")";
        val point: Point = reader.read(wktPoint).asInstanceOf[Point]
        //判断point属于哪一个区
        //循环遍历每一个区
        val rows: mutable.Buffer[Row] = wktPolygons.map(polygn => {
          //虚拟车站的点在当前区，给当前区进行加1
          if (polygn._2.contains(point)) {
            val fields = row.toSeq.toArray ++ Seq(polygn._1.getName)
            Row.fromSeq(fields)
          } else {
            null
          }
        }).filter(null != _)
        rows //过滤null
      })
    })

    //TODO 第五步：将每个区，有多少虚拟车站的数据保存到hbase
    val rowRDD: RDD[Row] = finalSaveRow.flatMap(x => x)
    HbaseTools.saveOrWriteData(hconf,rowRDD,Constants.VIRTUAL_STATION)
  }
}

/*
//getVirtualFrame方法封装了下面所有步骤
   //第二步：计算出所有的虚拟车站
   //h3
   val h3: H3Core = H3Core.newInstance
   //现在有了表，根据经纬度转换成hashcode,自定义函数locationToH3
   //[输入...，返回]
   sparkSession.udf.register("locationToH3",new UDF3[String,String,Int,Long] {
     override def call(t1: String, t2: String, t3: Int): Long = {
     //利用H3算法实现经纬度转hash
       h3.geoToH3(t1.toDouble,t2.toDouble,t3)
     }
   },DataTypes.LongType)
   val order_sql="select  order_id , city_id ," +
     "starting_lng,starting_lat,locationToH3(starting_lat,starting_lng,12) as h3code from order_df".stripMargin
   val frame: DataFrame = sparkSession.sql(order_sql)
   frame.createOrReplaceTempView("order_grid")

   //使用每个虚拟车站里面最小的一个经纬度，代表虚拟车站这个点
   //over（partition by class order by sroce） 按照sroce排序进行累计，order by是个默认的开窗函数，按照class分区。
   //自join，h3code相同的join到一起，组成一个六边形，经纬度最小的那个代表这个六边形
   val sql: String =
     s"""
        | select
        |order_id,
        |city_id,
        |starting_lng,
        |starting_lat,
        |row_number() over(partition by order_grid.h3code order by starting_lng,starting_lat asc) rn
        | from order_grid  join (
        | select h3code,count(1) as totalResult from order_grid  group by h3code having totalResult >=1
        | ) groupcount on order_grid.h3code = groupcount.h3code
   |having(rn=1)
   """.stripMargin
   //上面的sql语句，将每个经纬度转换成为了一个HashCode码值，然后对hashCode码值分组，
   // 获取每个组里面经纬度最小的那一个，得到这个经纬度，然后再计算，这个经纬度坐落在哪一个区里面
   val virtual_frame: DataFrame = sparkSession.sql(sql)
   //虚拟车站转rdd
   val virtual_rdd:RDD[Row] = virtual_frame.rdd*/
//上面的代码打开的话会报序列化的错

