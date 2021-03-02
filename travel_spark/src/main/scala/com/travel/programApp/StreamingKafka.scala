package com.travel.programApp

import com.travel.common.{ConfigUtil, Constants, JedisUtil}
import com.travel.utils.{HbaseTools}
import org.apache.hadoop.hbase.client.{Connection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{HasOffsetRanges,OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import redis.clients.jedis.Jedis


/**
 * 从kafka中读取成都，海口数据，写入hbase,并在hbase中保存offset
 * 并把成都的轨迹数据写入redis
 */
object StreamingKafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("StreamingKafka").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

      //broker不应该是地址值么？在哪里设置？在common包下的config.properties中
    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val topics = Array(ConfigUtil.getConfig(Constants.CHENG_DU_GPS_TOPIC),ConfigUtil.getConfig(Constants.HAI_KOU_GPS_TOPIC))
    //sparkstreaming属于的消费者组
    val group:String = "gps_consum_group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",// earliest,latest,和none
      //关闭自动提交offset到kafka，自己设置提交offset到hbase
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //TODO 第一步：消费kafka中数据，在hbase中保存偏移量，SparkStreaming程序根据偏移量到kafka中消费数据
    val resultDstream: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(ssc, kafkaParams, topics, group, "(.*)gps_topic")

    //可以设置事务，每处理一次数据提交一次offset
    //transcation start

    //TODO 第二步：将kafka中数据，保存到hbase和redis中
    resultDstream.foreachRDD(eachRdd=>{
      if(!eachRdd.isEmpty()){
        //遍历RDD的每个分区
        eachRdd.foreachPartition(eachPartition=>{
          val conn: Connection = HbaseTools.getHbaseConn
          val jedis: Jedis = JedisUtil.getJedis
          //每行数据
          eachPartition.foreach(eachLines=>{
            //将数据存入hbase和redies
            //hbase中存：成都和海口的全部数据
            //redis中存成都的经纬度信息（web页面轨迹监控中实时订单列表的展示根据这里的redis中的数据）
            HbaseTools.saveToHBaseAndRedis(conn,jedis,eachLines)
          })
          conn.close()
          jedis.close()
        })


        //TODO 第三步：更新hbase中的offset信息，每次消费完partition后，都要更新一次
//        OffsetRange表示从单个Kafka TopicPartition偏移量的范围
        val ranges: Array[OffsetRange] = eachRdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //自动提交offset到kafka
//        resultDstream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)

//        手动提交到hbase
        for (eachRage<-ranges){
          val startOffset: Long = eachRage.fromOffset
          val endOffset: Long = eachRage.untilOffset
          val topic: String = eachRage.topic
          val partition: Int = eachRage.partition

          //将offset值保存到hbase中
          HbaseTools.saveBatchOffset(group,topic,partition+"",endOffset)
        }

        //transcation end
      }
    })

//    添加监听,每隔5s监听一次
//    ssc.addStreamingListener(new SparkStreamingListener(5))
    ssc.start()
    ssc.awaitTermination()

  }
}

/*
    /**
     * SubscribePattern()消费两个topic中的数据，使用SubscribePattern。
     * topicPartitions: Iterable[TopicPartition],
     * kafkaParams: collection.Map[String, Object],
     * offsets: collection.Map[TopicPartition, Long]
     *  offsets中的TopicPartition代表：那个topic，那个partition
     *  offsets中的Long代表：偏移量
     */

      //获取hbase连接
    val conn: Connection = HbaseTools.getHbaseConn
    val admin: Admin = conn.getAdmin


      //第一次消费kafka时，访问hbase不存在这个偏移量表,进行表的创建和列族的设置
    if (!admin.tableExists(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))){
      val hbaseoffsetstoretable = new HTableDescriptor(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
      hbaseoffsetstoretable.addFamily(new HColumnDescriptor("f1"))
      admin.createTable(hbaseoffsetstoretable)
      admin.close()
    }
    val table: Table = conn.getTable(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))


    //设置SubscribePattern的offset参数
    val topicMap = new mutable.HashMap[TopicPartition, Long]()


    //根据rowkey获取表中数据,rowkey设计为group:topic
    //遍历topic
    for (eachTopic <- topics){
      val rowkey=group+":"+eachTopic
      //取一行数据
      val result: Result = table.get(new Get(rowkey.getBytes()))
      //获取每个分区partition的offset
      //得到一个个cell单元格，也就是我们表中的offset值
      val cells: Array[Cell] = result.rawCells()
      for(eachCell <- cells){
        //cloneQualifier方法：根据单元格取列名
        // 获取列名group1:topic:partition2
        val str: String = Bytes.toString(CellUtil.cloneQualifier(eachCell))
        //有了列名字，用":"切割得到partition
        val topicAndPartition: Array[String] = str.split(":")
        //获取单元格中offset值
        val strOffest: String = Bytes.toString(CellUtil.cloneValue(eachCell))

        val partition = new TopicPartition(topicAndPartition(1), topicAndPartition(2).toInt)
        topicMap += (partition->strOffest.toLong)
      }
    }

    val finalCounsumerStrategy=if (topicMap.size>0){
      //消费一次后，需要更新offset的值到hbase中，第二次消费时从hbase中获取offset值
      //将offset保存到hbase中，怎么设计表模型？hbase稀疏表，设计参考excel表中的设计
      ConsumerStrategies.SubscribePattern[String,String](Pattern.compile("(.*)gps_topic"),kafkaParams,topicMap)
    }else{
      //第一次启动Streaming程序，hbase中没有offset，从头消费kafka中数据
      ConsumerStrategies.SubscribePattern[String,String](Pattern.compile("(.*)gps_topic"),kafkaParams)
    }

    /**
     * createDirectStream()
     * ssc: StreamingContext,
     * locationStrategy: LocationStrategy,
     * consumerStrategy: ConsumerStrategy[K, V]
     */
    val resultDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, finalCounsumerStrategy)*/

//在HbaseTools.getStreamingContextFromHBase()方法中封装了上面所有的逻辑
