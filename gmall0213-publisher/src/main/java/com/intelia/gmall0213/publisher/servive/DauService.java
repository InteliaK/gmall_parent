package com.intelia.gmall0213.publisher.servive;

import java.util.Map;

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.24 6:54
 * @mogified By:
 */

public interface DauService {

    //日活总数
    public Long getDauTotal(String date);

    //小时活跃
    public Map getDauHourCount(String date);
}
