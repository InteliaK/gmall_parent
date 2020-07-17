package com.intelia.gmall0213.realtime.app

import com.intelia.gmall0213.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.17 15:41
 * @mogified By:
 */
object DauAPP {
    def main(args: Array[String]): Unit = {
        val sparkConf : SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))
        val grouoID = "dau_group"

        val recordInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("GMALL_START0213", ssc, grouoID)

        recordInputDstream.map(_.value()).print(1000)

        /*
        TODO
         1. 去重  把启动日志  只保留首次启动（日活）
            redis去重：set类型去重   String类型命令setnx去重
                    set把当日的记录保存子啊一个set中，便于管理，整体查询速度快
                    setnx把当日的记录保存在散列存储在redis，数据量大时，可以利用集群做分布式
         2.
         */

        recordInputDstream.filter{
            record =>
                val jsonString: String = record.value()
                null
        }

        ssc.start()
        ssc.awaitTermination()

    }

}
