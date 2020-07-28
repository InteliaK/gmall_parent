package com.intelia.gmall0213.realtime.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.27 7:09
 * @mogified By:
 */
object PhoenixUtil {
    def main(args: Array[String]): Unit = {
        val list : List[JSONObject] = queryList("select * from USER_STATE2020")

    }
    def queryList(sql : String ) : List[JSONObject] ={
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
        val resultList: ListBuffer[JSONObject] = new  ListBuffer[JSONObject]()



        val conn : Connection = DriverManager.getConnection("jdbc:phoenix:hadoop201,hadoop202,hadoop203:2181")
        val stat : Statement = conn.createStatement()
        val rs : ResultSet = stat.executeQuery(sql)
        val md : ResultSetMetaData = rs.getMetaData()
        while(rs.next()){
            val rowData = new JSONObject()
            for(i <- 1 to md.getColumnCount){
                rowData.put(md.getColumnName(i),rs.getObject(i))
            }
            resultList+=rowData
        }
        stat.close()
        conn.close()
        resultList.toList
    }
}
