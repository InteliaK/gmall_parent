package com.intelia.gmall0213.realtime.bean

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.27 6:06
 * @mogified By:
 */
case class OrderInfo(
                      id: Long,
                      province_id: Long,
                      order_status: String,
                      user_id: Long,
                      final_total_amount: Double,
                      benefit_reduce_amount: Double,
                      original_total_amount: Double,
                      feight_fee: Double,
                      expire_time: String,
                      create_time: String,
                      operate_time: String,
                      var create_date: String, // 把其他字段处理得到
                      var create_hour: String,

                      var if_first_order:String, //查询状态得到

                      var province_name:String,//查询维表得到
                      var province_area_code:String,
                      var province_3166_2_code:String,

                      var user_age_group:String,
                      var user_gender:String
                    )
