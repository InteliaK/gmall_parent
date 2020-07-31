package com.intelia.gmall0213.realtime.dws

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.intelia.gmall0213.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.intelia.gmall0213.realtime.util.{MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.29 4:11
 * @mogified By:
 */
object OrderWideApp {
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

        //
        //    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
        //    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))
        //
        //    val joinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream,4)



        // 方案一：  1开窗口    2 join 3  去重
        //window
        val orderInfoWindowDstream: DStream[OrderInfo] = orderInfoDstream.window(Seconds(15), Seconds(5)) //窗口大小  滑动步长
        val orderDetailWindowDstream: DStream[OrderDetail] = orderDetailDstream.window(Seconds(15), Seconds(5)) //窗口大小  滑动步长

        // join
        val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWindowDstream.map(orderInfo=>(orderInfo.id,orderInfo))
        val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailWindowDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))

        val joinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream,4)

        // 去重
        //  数据统一保存到
        // redis ?  type? set   api? sadd   key ? order_join:[orderId]        value ? orderDetailId (skuId也可）  expire : 60*10
        // sadd 返回如果0  过滤掉
        val orderWideDstream: DStream[OrderWide] = joinedDstream.mapPartitions { tupleItr =>
            val jedis: Jedis = RedisUtil.getJedisClient
            val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
            for ((orderId, (orderInfo, orderDetail)) <- tupleItr) {
                val key = "order_join:" + orderId
                val ifNotExisted: lang.Long = jedis.sadd(key, orderDetail.id.toString)
                jedis.expire(key, 600)
                //合并宽表
                if (ifNotExisted == 1L) {
                    orderWideList.append(new OrderWide(orderInfo, orderDetail))
                }
            }
            jedis.close()
            orderWideList.toIterator
        }

        //orderWideDstream.print(1000)

        // //按比例求分摊: 分摊金额/实际付款金额 = 个数*单价  /原始总金额
        // 分摊金额= 实际付款金额 *(个数*单价) / 原始总金额   （乘除法，适用非最后一笔明细)
        //         最后一笔 = 实际付款金额-  Σ其他的明细的分摊金额  (减法，适用最后一笔明细）
        // 如何判断最后一笔 ？如果当前的一笔 个数*单价== 原始总金额-  Σ其他的明细（个数*单价）
        //   利用 redis(mysql)  保存计算完成的累计值  Σ其他的明细的分摊金额 Σ其他的明细（个数*单价）

        orderWideDstream.mapPartitions{ orderWideItr =>
            val jedisClient: Jedis = RedisUtil.getJedisClient
            for(orderWide <- orderWideItr){

                //获取数据
                val original_total_amount : Double = orderWide.original_total_amount  //原始总金额
                val final_total_amount: Double = orderWide.final_total_amount   //实际付款金额

                val price: Double = orderWide.sku_price
                val num: Long = orderWide.sku_num
                val order_allocation : Double= price * num

                //分摊金额累积
                val order_origin_sum_key : String= "order_origin_sum" + orderWide.order_id
                var order_origin_sum: Double = jedisClient.get(order_origin_sum_key).toDouble
                //原始金额累积
                val order_allocation_sum_key : String = "order_allocation_sum" + orderWide.order_id
                var order_allocation_sum: Double = jedisClient.get(order_allocation_sum_key).toDouble
                if(order_allocation_sum != null && order_origin_sum != null){
                    //判断订单是否是最后一笔并计算分摊金额
                    if( order_allocation == original_total_amount - order_allocation_sum){
                        orderWide.final_detail_amount = final_total_amount - order_origin_sum
                    }
                }else{
                    orderWide.final_detail_amount = final_total_amount * (num * price) / original_total_amount
                }
                //将分摊金额累积到redis中
                order_origin_sum = order_origin_sum + orderWide.final_detail_amount
                order_allocation_sum = order_allocation_sum + order_allocation

                jedisClient.set(order_origin_sum_key,order_origin_sum.toString)
                jedisClient.set(order_allocation_sum_key,order_allocation_sum.toString)

                jedisClient.expire(order_origin_sum_key,1000)
                jedisClient.expire(order_allocation_sum_key,1000)
            }
            jedisClient.close()
            orderWideItr
        }




        //jdbc sql

        val sparkSession: SparkSession = SparkSession.builder().appName("dws_order_wide_app")getOrCreate()

        import  sparkSession.implicits._
        orderWideDstream.foreachRDD{rdd=>
            val df: DataFrame = rdd.toDF()
            df.write.mode(SaveMode.Append)
              .option("batchsize", "100")
              .option("isolationLevel", "NONE") // 设置事务
              .option("numPartitions", "4") // 设置并发
              .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
              .jdbc("jdbc:clickhouse://hadoop201:8123/gmall_dws","order_wide_0213",new Properties())
            println("连接成功")
            OffsetManager.saveOffset(orderInfoTopic,orderInfoGroupId,orderInfoOffsetRanges)
            OffsetManager.saveOffset(orderDetailTopic,orderDetailGroupId,orderDetailOffsetRanges)
        }
        ssc.start()
        ssc.awaitTermination()
    }
}
