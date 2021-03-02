package com.travel.programApp.hbaseSource

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType
//with SupportsPushDownFilters with SupportsPushDownRequiredColumns实现谓词下推和列剪枝的两个类
class HBaseDataSourceReader(tableName:String,schema:String,cfcc:String) extends DataSourceReader /*with SupportsPushDownFilters with SupportsPushDownRequiredColumns*/{

  val structType: StructType = StructType.fromDDL(schema)

  override def readSchema(): StructType = {
    structType
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {

    import scala.collection.JavaConverters._
    Seq(
    new HBaseDataReaderFactory(tableName,cfcc).asInstanceOf[DataReaderFactory[Row]]
    ).asJava
  }

/*//  谓词下推和列剪枝的实现
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
//    给各种各样的filter，进行数据过滤
//    age>70在sql中写一遍（在executor中执行），在Filter中也写一遍条件（谓词下推到HRegionServer）。


  }

  override def pushedFilters(): Array[Filter] = {

  }

  override def pruneColumns(requiredSchema: StructType): Unit = {

  }*/

}
