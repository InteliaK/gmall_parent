package com.intelia.gmall0213.log.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.util.Date;

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.23 5:53
 * @mogified By:
 */
@RestController
public class DictController {
    @GetMapping("dict")
    public String dict(HttpServletResponse response){

        response.addHeader("Last-Modified",new Date().toString());

        return "因媞莉亚";
    }
}
