package com.intelia.gmall0213.realtime.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.17 20:48
 * @mogified By:
 */
object PropertiesUtil {

    def main(args: Array[String]): Unit = {
        val properties: Properties = PropertiesUtil.load("config.properties")

        println(properties.getProperty("kafka.broker.list"))
    }

    def load(propertieName: String): Properties = {
        val prop = new Properties();
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
        prop
    }

}
