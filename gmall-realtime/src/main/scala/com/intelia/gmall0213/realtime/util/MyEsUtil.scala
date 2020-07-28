package com.intelia.gmall0213.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

import scala.collection.JavaConverters._

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.23 7:10
 * @mogified By:
 */
object MyEsUtil {
    private var factor: JestClientFactory = null

    def getJestClient() = {
        if (factor != null) {
            factor.getObject
        } else {
            build()
            factor.getObject
        }
    }

    def build() = {
        factor = new JestClientFactory()
        factor.setHttpClientConfig(
            new HttpClientConfig
            .Builder("http://hadoop201:9200")
              .multiThreaded(true)
              .maxTotalConnection(20)
              .connTimeout(1000)
              .readTimeout(1000)
              .build()
        )

    }

    //单条写入   iO频繁   产生较多segment
    def saveToEs(args: Array[String]): Unit = {
        //saveToEs()
        //query  操作
        val jestClient: JestClient = getJestClient()
        val index = new Index
        .Builder(movie("0103", "功夫"))
          .index("movie_test_20200723")
          .`type`("_doc")
          .id("123")
          .build()
        jestClient.execute(index)
        jestClient.close()
    }

    //批次化操作
    def bulkSave(list: List[(Any, String)], indexName: String) = {
        if (list != null && list.size > 0) {
            val jestClient: JestClient = getJestClient()
            val bulkBuilder = new Bulk.Builder
            bulkBuilder.defaultIndex(indexName).defaultType("_doc")
            for ((doc, id) <- list) {
                //如果给id指定id 幂等性（保证精确一次消费的必要条件） //不指定id 随机生成 非幂等性
                val index = new Index.Builder(doc).id(id).build()
                bulkBuilder.addAction(index)
            }
            val bulk: Bulk = bulkBuilder.build()
            val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(bulk).getItems
            jestClient.close()
        }
    }

    def queryFromEs() = {
        val jestClient = getJestClient()
        val query = "{\n  \"query\": {\n    \"match\": {\n      \"name\": \"red\"\n    }\n  },\n  \"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"desc\"\n      }\n    }\n  ],\n  \"from\": 0,\n  \"size\": 20\n}"
        val searchSourceBuilder = new SearchSourceBuilder
        searchSourceBuilder.query(new MatchQueryBuilder("name", "red"))
        searchSourceBuilder.sort("doubanScore", SortOrder.ASC)
        searchSourceBuilder.from(0)
        searchSourceBuilder.size(20)
        val query2: String = searchSourceBuilder.toString
        println(query2)
        val search: Search = new Search
        .Builder(query2)
          .addIndex("movie_index")
          .addType("movie")
          .build()
        val result: SearchResult = jestClient.execute(search)
        val resultList: util.List[SearchResult#Hit[util.Map[String, Object], Void]] = result.getHits(classOf[util.Map[String, Object]])
        import collection.JavaConversions._
        for (hit <- resultList) {
            val source = hit.source
            println(source)
        }
        jestClient.close()
    }

    def main(args: Array[String]): Unit = {
        queryFromEs()
    }

    case class movie(id: String, name: String)

}
