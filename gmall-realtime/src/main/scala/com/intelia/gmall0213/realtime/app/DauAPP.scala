package com.intelia.gmall0213.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.intelia.gmall0213.realtime.bean.DauInfo
import com.intelia.gmall0213.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.17 15:41
 * @mogified By:
 */
object DauAPP {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val grouoID = "dau_group"
        val topic = "GMALL_START0213"


        //TODO 手动偏移量
        //1. 从redis中读取偏移量(启动一次执行)
        val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(grouoID, topic)
        //2. 把偏移量交给kafka，加载数据流（启动一次执行）
        var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetMapForKafka != null && offsetMapForKafka.size > 0) {
            recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMapForKafka, grouoID)
        } else {
            recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, grouoID)
        }


        //3. 从流中获得本批次的偏移量结束点（每批次执行一次）
        var offsetRanges: Array[OffsetRange] = null //周期性存储当前偏移量的变化状态
        val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
            rdd => //周期性在driver中执行
                println(1111)
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //获得当前偏移量状态
                rdd
        }

        println(2222)
        //4. 把偏移量结束点写入redis中（每批次执行一次）


        //recordInputDstream.map(_.value()).print(1000)

        /*
        TODO
         1. 去重  把启动日志  只保留首次启动（日活）
            redis去重：set类型去重   String类型命令setnx去重
                    set把当日的记录保存子啊一个set中，便于管理，整体查询速度快
                    setnx把当日的记录保存在散列存储在redis，数据量大时，可以利用集群做分布式
         2.
         */

        val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map {
            record =>
                val jsonString: String = record.value()
                val jsonObject: JSONObject = JSON.parseObject(jsonString)
                jsonObject
        }
        val jsonObjFilteredDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions {
            jsonObjItr =>
                val beforeFilteredlist: List[JSONObject] = jsonObjItr.toList
                println("过滤前：" + beforeFilteredlist.size)
                val jsonObjList = new ListBuffer[JSONObject]

                val jedis: Jedis = RedisUtil.getJedisClient
                for (jsonObj <- beforeFilteredlist) {
                    val mid: String = jsonObj.getJSONObject("common").getString("mid")
                    val ts: lang.Long = jsonObj.getLong("ts")
                    val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
                    //key :  dau: + dt          value :  mid
                    val key = "dau:" + dt
                    val flag: lang.Long = jedis.sadd(key, mid)

                    //判断返回值
                    if (flag == 1L) {
                        jsonObjList.append(jsonObj)
                    }
                }
                jedis.close()
                println("过滤后：" + jsonObjList.size)
                jsonObjList.toIterator
        }

        println(3333)

        //        jsonObjFilteredDstream.print(1000)
        jsonObjFilteredDstream.foreachRDD {
            rdd =>
                //                rdd.foreach(jsonObj => println(jsonObj))//写入数据库操作
                rdd.foreachPartition {
                    jsonObjItr =>
                        val jsonObjectList: List[JSONObject] = jsonObjItr.toList
                        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm")
                        val dauWithList: List[(DauInfo, String)] = jsonObjectList.map {
                            jsonObj =>
                                val ts: lang.Long = jsonObj.getLong("ts")
                                val dateTimeString: String = format.format(new Date(ts))
                                val dateTimeArr: Array[String] = dateTimeString.split(" ")
                                val dt: String = dateTimeArr(0)
                                val timeArr: Array[String] = dateTimeArr(1).split(":")
                                val hr: String = timeArr(0)
                                val mi: String = timeArr(1)

                                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                                val dauInfo = DauInfo(
                                    commonJsonObj.getString("mid"),
                                    commonJsonObj.getString("uid"),
                                    commonJsonObj.getString("ar"),
                                    commonJsonObj.getString("ch"),
                                    commonJsonObj.getString("vc"),
                                    dt, hr, mi, ts
                                )
                                (dauInfo, dauInfo.mid)
                        }
                        val today: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
                        println(today)
                        MyEsUtil.bulkSave(dauWithList, "gmall_dau_info_" + today)
                }
                OffsetManager.saveOffset(topic, grouoID, offsetRanges) //要在driver中执行 周期性 每批执行一次
        }


        ssc.start()
        ssc.awaitTermination()

    }

}
