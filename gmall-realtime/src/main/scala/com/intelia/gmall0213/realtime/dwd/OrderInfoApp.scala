package com.intelia.gmall0213.realtime.dwd

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import com.intelia.gmall0213.realtime.bean.{OrderInfo, UserState}
import com.intelia.gmall0213.realtime.util.{MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.27 6:09
 * @mogified By:
 */
object OrderInfoApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("base_db_canal_app")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val groupId = "base_db_canal_group"
        val topic = "ODS_ORDER_INFO";

        //1   从redis中读取偏移量   （启动执行一次）
        val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)

        //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
        var recordInputDstream: InputDStream[ConsumerRecord[String, String]]=null
        //根据是否能取到偏移量来决定如何加载kafka 流
        if(offsetMapForKafka!= null && offsetMapForKafka.size>0){
            recordInputDstream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMapForKafka,groupId )
        }else{
            recordInputDstream = MyKafkaUtil.getKafkaStream(topic,ssc, groupId )
        }


        //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
        var offsetRanges: Array[OffsetRange]=null    //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
        val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform { rdd => //周期性在driver中执行
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }
        
        //TODO 数据提取
        val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map { record =>
            val jsonString: String = record.value()
            //订单处理 脱敏  转换为更方便操作的样例类
            val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
            val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
            orderInfo.create_date = createTimeArr(0)
            orderInfo.create_hour = createTimeArr(1).split(":")(0)
            orderInfo
        }

        //查询Phoenix
        val orderInfoWithFlagDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
            val orderInfoList: List[OrderInfo] = orderInfoItr.toList
            if (orderInfoList != null && orderInfoList.size > 0) {
                val userIdList: List[Long] = orderInfoList.map(orderInfo => orderInfo.user_id)

                val sql =
                    s"""
                       |select
                       |  USER_ID,
                       |  IF_CONSUMED
                       |from USER_STATE0213
                       |where
                       |  USER_ID in ('${userIdList.mkString(",")}')
                       |  """.stripMargin
                val ifConsumedList: List[JSONObject] = PhoenixUtil.queryList(sql)
                val ifConsumedMap: Map[String, String] = ifConsumedList.map(jsonObj =>
                    (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED"))).toMap
                for (orderInfo <- orderInfoList) {
                    val ifConsumed: String = ifConsumedMap.getOrElse(orderInfo.user_id.toString, "0")
                    if (ifConsumed == "1") {
                        orderInfo.if_first_order = "0"
                    } else {
                        orderInfo.if_first_order = "1"
                    }
                }

            }
            orderInfoList.toIterator

        }

        //单批次单人多个订单首冲去重
        val orderInfoGroupByUserIdDstream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithFlagDstream.map(orderInfo => (orderInfo.user_id,orderInfo)).groupByKey()
        val orderInfoRealWithFirstFlagDstream: DStream[OrderInfo] = orderInfoGroupByUserIdDstream.flatMap {
            case (userId, orderInfoItr) => {
                var orderList: List[OrderInfo] = orderInfoItr.toList
                if (orderList != null && orderList.nonEmpty) {
                    val orderInfoAny: OrderInfo = orderList(0)
                    if (orderList.size > 2 && orderInfoAny.if_first_order == "1") {
                        val sortList: List[OrderInfo] = orderList.sortWith((orderInfo1, orderInfo2) => orderInfo1.create_time < orderInfo2.create_time)
                        for (i <- 1 until sortList.size) {
                            val orderInfoNotFirstThisBatch: OrderInfo = sortList(i)
                            orderInfoNotFirstThisBatch.if_first_order = "0"
                        }
                        sortList
                    } else {
                        orderList
                    }
                } else {
                    orderList
                }
            }
        }


        ////////////
        //关联省份维度
        val orderInfoWithProvinceDstream: DStream[OrderInfo] = orderInfoRealWithFirstFlagDstream.transform { rdd =>

            //在Phoenix中查询，转换为map，设为广播变量
            val sql =
                """
                  |select
                  |  ID,
                  |  NAME,
                  |  AREA_CODE,
                  |  ISO_3166_2
                  |from
                  |  GMALL0213_PROVINCE_INFO
                  |""".stripMargin
            val provinceList: List[JSONObject] = PhoenixUtil.queryList(sql)
            val provinceMap: Map[lang.Long, JSONObject] = provinceList.map { jsonObj => (jsonObj.getLong("ID"), jsonObj) }.toMap

            val provinceMapBC: Broadcast[Map[lang.Long, JSONObject]] = ssc.sparkContext.broadcast(provinceMap)

            val rddWithProvince: RDD[OrderInfo] = rdd.map { orderInfo =>
                val provinceMap: Map[lang.Long, JSONObject] = provinceMapBC.value
                val provinceJsonObj: JSONObject = provinceMap.getOrElse(orderInfo.user_id, null)
                if (provinceJsonObj != null) {
                    orderInfo.province_name = provinceJsonObj.getString("NAME")
                    orderInfo.province_area_code = provinceJsonObj.getString("AREA_CODE")
                    orderInfo.province_3166_2_code = provinceJsonObj.getString("ISO_3166_2")
                }
                orderInfo
            }
            rddWithProvince
        }

        //写入操作
        //1存储到Phoenix中

        orderInfoWithProvinceDstream.foreachRDD{rdd =>
            rdd.cache()
            val userStateRDD: RDD[UserState] = rdd.map(orderInfo =>UserState(orderInfo.user_id.toString,"1"))
            import org.apache.phoenix.spark._
            userStateRDD.saveToPhoenix(
                "USER_STATE0213",
                Seq("USER_ID","IF_CONSUMED"),
                new Configuration,
                Some("hadoop201,hadoop202,hadoop203：2181")
            )
            //2  存储olap  用户分析    可选  es
            //3  推kafka 进入下一层处理   可选  主题： DWD_ORDER_INFO
            rdd.foreachPartition{orderInfoItr =>
                val orderInfoList : List[(OrderInfo,String)] = orderInfoItr.toList.map(orderInfo => (orderInfo,orderInfo.id.toString))

            }
            //4  提交偏移量
        }

        ssc.start()
        ssc.awaitTermination()
    }   
}
