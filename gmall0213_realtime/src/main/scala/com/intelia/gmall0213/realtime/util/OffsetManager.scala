package com.intelia.gmall0213.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis
import  scala.collection.JavaConversions._
/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.20 5:33
 * @mogified By:
 */
object OffsetManager {
//    def getOffset(groupId:String,topic:String):Map[TopicPartition,Long]={
//        var offsetMap=Map[TopicPartition,Long]()
//
//        val jedisClient: Jedis = RedisUtil.getJedisClient
//
//        val redisOffsetMap: util.Map[String, String] = jedisClient.hgetAll("offset:"+groupId+":"+topic)
//        jedisClient.close()
//        if(redisOffsetMap!=null&&redisOffsetMap.isEmpty){
//            null
//        }else {
//
//            val redisOffsetList: List[(String, String)] = redisOffsetMap.toList
//
//            val kafkaOffsetList: List[(TopicPartition, Long)] = redisOffsetList.map { case ( partition, offset) =>
//                (new TopicPartition(topic, partition.toInt), offset.toLong)
//            }
//            kafkaOffsetList.toMap
//        }
//    }

    def getOffset(groupID:String, topic:String):Map[TopicPartition,Long]={
        val jedis: Jedis = RedisUtil.getJedisClient
        val offsetMap: util.Map[String, String] = jedis.hgetAll(topic+":"+groupID)
        jedis.close()
        if(offsetMap != null && offsetMap.size > 0){
            offsetMap
              .toList
              .map{
                case (partition,offset) =>
                    (new TopicPartition(topic,partition.toInt),offset.toLong)
            }.toMap
        }else{
            null
        }
    }

    def main(args: Array[String]): Unit = {
        println(getOffset("dau_group","GMALL_START0213"))
    }

    def saveOffset(groupId:String,topic:String ,offsetArray:Array[OffsetRange]):Unit= {
        if (offsetArray != null && offsetArray.size > 0) {
            val offsetMap: Map[String, String] = offsetArray.map { offsetRange =>
                val partition: Int = offsetRange.partition
                val untilOffset: Long = offsetRange.untilOffset
                (partition.toString, untilOffset.toString)
            }.toMap

            val jedisClient: Jedis = RedisUtil.getJedisClient

            jedisClient.hmset("offset:" + groupId + ":" + topic, offsetMap)
            jedisClient.close()
        }
    }

}
