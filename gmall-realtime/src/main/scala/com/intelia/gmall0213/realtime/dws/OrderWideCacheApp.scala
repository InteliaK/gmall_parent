package com.intelia.gmall0213.realtime.dws

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.intelia.gmall0213.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.intelia.gmall0213.realtime.util.{MyKafkaUtil, OffsetManager, RedisUtil}
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
 * @date 2020.7.31 3:55
 * @mogified By:
 */
object OrderWideCacheApp {
    def main(args: Array[String]): Unit = {
        //双流  订单主表  订单从表    偏移量 双份
        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dws_order_wide_app")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val orderInfoGroupId = "dws_order_info_group"
        val orderInfoTopic = "DWD_ORDER_INFO"
        val orderDetailGroupId = "dws_order_detail_group"
        val orderDetailTopic = "DWD_ORDER_DETAIL"

        //1   从redis中读取偏移量   （启动执行一次）
        val orderInfoOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(orderInfoTopic, orderInfoGroupId)
        val orderDetailOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(orderDetailTopic, orderDetailGroupId)

        //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
        var orderInfoRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
        if (orderInfoOffsetMapForKafka != null && orderInfoOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
            orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMapForKafka, orderInfoGroupId)
        } else {
            orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
        }


        var orderDetailRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
        if (orderDetailOffsetMapForKafka != null && orderDetailOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
            orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMapForKafka, orderDetailGroupId)
        } else {
            orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
        }


        //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
        var orderInfoOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
        val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputDstream.transform { rdd => //周期性在driver中执行
            orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }

        var orderDetailOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
        val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputDstream.transform { rdd => //周期性在driver中执行
            orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }

        // 1 提取数据 2 分topic
        val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
            val jsonString: String = record.value()
            //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
            val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
            orderInfo
        }

        val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
            val jsonString: String = record.value()
            //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
            val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
            orderDetail
        }



        val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo => (orderInfo.id,orderInfo))
        val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail => (orderDetail.id,orderDetail))

        val fullJoinedDstream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream,4)

        val orderWideDstream: DStream[ListBuffer[OrderWide]] = fullJoinedDstream.map { case (orderId, (orderInfoOpt, orderDetailOpt)) =>

            val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
            val jedis: Jedis = RedisUtil.getJedisClient

            if (orderInfoOpt != None) {
                val orderInfo: OrderInfo = orderInfoOpt.get
                if (orderDetailOpt != None) {
                    val orderDetail: OrderDetail = orderDetailOpt.get
                    val orderWide = new OrderWide(orderInfo, orderDetail)
                    orderWideList.append(orderWide)
                }
                //TODO 主表 写缓存
                val orderInfoKey = "order_Info" + orderInfo.id
                //序列化
                val orderInfoJson: String = JSON.toJSONString(orderInfo, new SerializeConfig(true))
                jedis.setex(orderInfoKey, 600, orderInfoJson)

                //TODO 主表 读缓存
                //从表  redis  存储结构  set、list   key order_detail + orderInfo.id  value  多个orderDetailJSON
                val orderDetailKey = "order_detail" + orderInfo.id
                val orderDetailJsonSet: util.Set[String] = jedis.smembers(orderDetailKey)
                import collection.JavaConversions._
                for (orderDetailJson <- orderDetailJsonSet) {
                    val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                    val orderWide = new OrderWide(orderInfo, orderDetail)
                    orderWideList.append(orderWide)
                }
            } else {
                val orderDetail: OrderDetail = orderDetailOpt.get
                //TODO 从表  读缓存
                val orderInfoKey = "orderInfo" + orderDetail.order_id
                val orderInfoJson: String = jedis.get(orderInfoKey)
                if (orderInfoJson != null && orderInfoJson.length > 0) {
                    val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
                    val orderWide = new OrderWide(orderInfo, orderDetail)
                    orderWideList.append(orderWide)
                }
                //TODO 从表  写缓存
                val orderDetailKey = "order_detail" + orderDetail.order_id
                val orderDetailJson: String = JSON.toJSONString(orderDetail, new SerializeConfig(true))
                jedis.sadd(orderDetailKey, orderDetailJson)
                jedis.expire(orderDetailKey, 600)
            }

            jedis.close()
            orderWideList
        }
        orderWideDstream.print(1000)
    }
}
