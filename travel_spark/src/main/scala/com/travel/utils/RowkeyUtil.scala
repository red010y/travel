package com.travel.utils

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.StringUtils


object RowkeyUtil {


  def getRowKey(str:String,numRegion:Int):String ={
//    numRegion表示有多少个预分区，result结果值0-7
    val result: Int = (str.hashCode & Integer.MAX_VALUE) % numRegion
    //左补全：0000 0001 0002 0003 0004 0005 0006 0007
    val prefix:String = StringUtils.leftPad(result+"",4,"0");
//    将rowkey进行md5加密，之后进行截串，前12位
    val suffix: String = DigestUtils.md5Hex(str).substring(0,12)
//    拼上前缀
    prefix + suffix
  }




  def main(args: Array[String]): Unit = {
    val str: String = getRowKey("1_20200120180752",8)
    println(str)
  }

}
