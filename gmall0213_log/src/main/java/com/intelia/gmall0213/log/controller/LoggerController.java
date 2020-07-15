package com.intelia.gmall0213.log.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.16 5:46
 * @mogified By:
 */
@RestController   //= @controller + @response
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String applog(@RequestBody String json){
        System.out.println(json);
        JSONObject jsonObject = JSON.parseObject(json);
        if(jsonObject.getString("start") != null && jsonObject.getString("start").length()>0){
            kafkaTemplate.send("GMALL_START0213",json);
        }else{
            kafkaTemplate.send("GMALL_EVENT0213",json);
        }

        return "success";
    }
}
