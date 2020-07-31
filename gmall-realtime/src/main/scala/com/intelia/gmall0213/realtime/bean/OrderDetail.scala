package com.intelia.gmall0213.realtime.bean

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.29 4:16
 * @mogified By:
 */
case class OrderDetail(
                      id : Long,
                      order_id:Long,
                      sku_id: Long,
                      order_price: Double,
                      sku_num:Long,
                      sku_name: String,
                      create_time: String,

                      var spu_id: Long,     //作为维度数据 要关联进来
                      var tm_id: Long,
                      var category3_id: Long,
                      var spu_name: String,
                      var tm_name: String,
                      var category3_name: String
                      )
