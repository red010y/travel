package com.travel.programApp

import java.util
import java.util.Optional

import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object HBaseSourceAndSink {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("sparkSQLSourceAndSink")
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    //TODO 第一步：spark读自定义数据源（这里读取的是HBASE中的数据）
    //    spark读取数据spark.read.Xxx（json，csv）
    //    format需要我们自定义数据源（"class类路径"），反射
    val df: DataFrame = spark.read.format("com.travel.programApp.HBaseSource")
      //我们自己带的一些参数
      .option("hbase.table.name", "spark_hbase_sql") //查询哪张表
      .option("cf.cc", "cf:name,cf:score") //定义我们查询habse的哪些列
      .option("scheam", "`name` STRING,`score` STRING") //定义我们 表的schema
      .load//加载数据

    //TODO 把从hbase中读取的数据创建sparkSQL的临时表
    df.createOrReplaceTempView("sparkHbaseSQL")
    df.printSchema()

    //TODO 进行sql分析得到的结果数据，将结果数据，保存到hbase，redis或者mysql或者es等等都行
    val resultDF: DataFrame = spark.sql("select * from sparkHbaseSQL where score > 70 ")
    resultDF.write.format("com.travel.programApp.HBaseSource").mode(SaveMode.Overwrite)
      .option("hbase.table.name","spark_hbase_write")
      .option("cf","cf")
      .save()
  }
}

//TODO 第二步：自定义数据源继承DataSourceV2
//   可以自定义数据源，实现数据的查询，和写入
class HBaseSource extends DataSourceV2 with  ReadSupport with WriteSupport{
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val tableName: String = options.get("hbase.table.name").get()
    val cfAndCC: String = options.get("cf.cc").get()
    val schema: String = options.get("scheam").get()

    new HBaseDataSourceReader(tableName,cfAndCC,schema)

  }

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new HBaseDataSourceWriter )
  }

}


class HBaseDataSourceReader(tableName:String,cfAndCC:String,schema:String) extends DataSourceReader{
  /**
   * 定义我们映射的表的schema
   * @return
   */
  override def readSchema(): StructType = {
    StructType.fromDDL(schema)
  }

  //  util.List是java集合。util全部都是java里面的
  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    //java集合和scala集合互转的包
    import scala.collection.JavaConverters._
    //    asInstanceOf强转成他要的DataReaderFactory[Row]
    Seq(new HBaseDataReaderFactory(tableName,cfAndCC).asInstanceOf[DataReaderFactory[Row]]).asJava
  }

}


class HBaseDataReaderFactory(tableName:String,cfAndCC:String) extends DataReaderFactory[Row]{
  override def createDataReader(): DataReader[Row] = {
    new HBaseDataReader(tableName,cfAndCC)
  }
}


/**
 *
 * 自定义source查询redis以及es以及其他的数据库的数据？？？
 * 做一个大数据的查询平台，不管你是什么样的数据源，都可以通过sql去查询数据
 *
 *  自定义HBaseDataReader 实现了dataReader接口
 */
class HBaseDataReader(tableName:String,cfcc:String) extends DataReader[Row]{

  var conn: Connection = null
  var table: Table = null
  var scan = new Scan()
  var resultScanner: ResultScanner = null

  //获取我们hbase的数据就在这
  def getIterator: Iterator[Seq[AnyRef]] = {
    /*另一种方案：
        不能使用newAPIHadoopRDD读取hbase中的数据
        Option中不能传对象，只能传字符串
        sparkContext.newAPIHadoopRDD
    //    使用ProtobufUtil将sparkContext对象序列化成为一个字符串传下来，下面再给反序列化可以试试看
        */


    conn = HbaseTools.getHbaseConn
    table = conn.getTable(TableName.valueOf(tableName))
    //    还是用scanner取出数据，这种为什么比直接用scanner性能好  涉及到rdd的底层，可以直接做列剪枝，以及谓词下推到服务端去执行
    resultScanner = table.getScanner(scan)

    //java集合和scala集合互转的包
    import scala.collection.JavaConverters._
    //asScala转成scala集合，获取到了每一条数据
    val iterator: Iterator[Seq[AnyRef]] = resultScanner.iterator().asScala.map(eachResult => {
      //      val strings: Array[String] = cfcc.split(",")
      //      从cfcc中获取列族，列名。替换掉name，score

      //      把每条数据封装成Iterator里面装着Seq
      val name: String = Bytes.toString(eachResult.getValue("cf".getBytes(), "name".getBytes()))
      val score: String = Bytes.toString(eachResult.getValue("cf".getBytes(), "score".getBytes()))
      Seq(name, score)
    })
    iterator
  }

  //获取hbase中的数据
  val data:Iterator[Seq[AnyRef]] = getIterator

  /**
   *这个方法反复不断的被调用，只要我们查询到了数据，就可以使用next方法一直获取下一条数据
   * data.hasNext()
   * data.next
   * @return
   */
  override def next(): Boolean = {
    //    怎么知道的next（）里面让通过迭代器一直一直往下走的？
    //    看DataReader源码
    data.hasNext

  }
  /**
   * next判断到数据后，调用get方法
   * 获取到的数据在这个方法里面一条条的解析，解析之后，映射到我们提前定义的表里面去
   * @return
   */
  override def get(): Row = {
    val seq: Seq[AnyRef] = data.next()
    Row.fromSeq(seq)
  }
  /**
   * 关闭一些资源的
   */
  override def close(): Unit = {
    table.close()
    conn.close()
  }
}


//--------------------------------------Writer自定义数据源---------------------------------------------------------------
class HBaseDataSourceWriter extends DataSourceWriter{
  /**
   * 将我们的数据保存起来，全部依靠这个方法
   * @return
   */
  override def createWriterFactory(): DataWriterFactory[Row] = {
    new HBaseDataWriterFactory
  }

  //提交数据时候带的一些注释信息
  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  //数据插入失败的时候带的一些注释信息
  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}


class HBaseDataWriterFactory extends DataWriterFactory[Row]{
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new HBaseDataWriter
  }
}


class HBaseDataWriter extends DataWriter[Row]{

  private val conn: Connection = HbaseTools.getHbaseConn
  //表名可以放到构造器中传下来
  private val table: Table = conn.getTable(TableName.valueOf("spark_hbase_write"))
  //写入数据
  override def write(record: Row): Unit = {
    //    一条条数据就是封装在ROW对象中
    val name: String = record.getString(0)
    val score: String = record.getString(1)

    val put = new Put("0001".getBytes())
    put.addColumn("cf".getBytes(),"name".getBytes(),name.getBytes())
    put.addColumn("cf".getBytes(),"score".getBytes(),score.getBytes())

    table.put(put)
  }

  //数据的提交方法，数据插入完成之后，在这个方法里面进行数据的事务提交
  //  这里没有涉及到事务
  override def commit(): WriterCommitMessage = {
    table.close()
    conn.close()
    null


  }

  override def abort(): Unit = {

  }
}



