package cn.yintech.esUtil;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class EsHighRrestClientAggregateTest {


    public static void main(String[] args) throws IOException, InterruptedException {

        RestHighLevelClient client = ESConfig.client();
        // bool查询
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        // bool子查询：范围查询
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("logtime"); //新建range条件
//        rangeQueryBuilder.gte("now-3m"); //开始时间，时间范围
//        SimpleDateFormat  sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
//        sdf.setTimeZone(TimeZone.getTimeZone("UTC-0"));
//        rangeQueryBuilder.gte(sdf.format(new Date())+"Z"); //开始时间
//        rangeQueryBuilder.lt(sdf.format(new Date())+"Z"); //结束时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        rangeQueryBuilder.gte(sdf.format(new Date())+"T00:00:00+08:00"); //开始时间
        rangeQueryBuilder.lt("now"); //结束时间
        boolBuilder.must(rangeQueryBuilder);
        // bool子查询：字符匹配查询
        boolBuilder.must(QueryBuilders.matchQuery("is_online", "1"));
        boolBuilder.should(QueryBuilders.matchQuery("type", "圈子视频直播"));
        boolBuilder.should(QueryBuilders.matchQuery("type", "圈子文字直播"));
        boolBuilder.minimumShouldMatch(1);


        // 结果排序
//        SortBuilder sortBuilder = SortBuilders.fieldSort("@timestamp").order(SortOrder.ASC);// 排训规则
        // 新建查询源
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolBuilder); //设置查询，可以是任何类型的QueryBuilder。
        sourceBuilder.from(0); //设置确定结果要从哪个索引开始搜索的from选项，默认为0
        sourceBuilder.size(10000); //设置确定搜素命中返回数的size选项，默认为10
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); //设置一个可选的超时，控制允许搜索的时间。
        // 聚合操作
        AggregationBuilder aggs1 =  AggregationBuilders.terms("extra_id_dist").field("extra_id").size(10000);
        AggregationBuilder aggs2 =  AggregationBuilders.terms("type_count").field("type");
        AggregationBuilder aggs3 = AggregationBuilders.cardinality("device_id_count").field("device_id");
//        aggs1.subAggregation(aggs2);
        aggs1.subAggregation(aggs2.subAggregation(aggs3));
        sourceBuilder.aggregation(aggs1);
//        sourceBuilder.sort(sortBuilder);
        // 第一个是获取字段，第二个是过滤的字段，默认获取全部
        //sourceBuilder.fetchSource(new String[] {"fields.port","fields.entity_id","fields.message"}, new String[] {});
        /**
         * 制定索引库和类型表
         */
        SearchRequest searchRequest = new SearchRequest("real_time_count"); //索引：接口库——success
        searchRequest.types("real_time_count"); //类型:接口请求记录
        searchRequest.source(sourceBuilder);
        // 深度查询：分页查询全量数据
//        List<SearchHit> searchHits = ESConfig.scrollSearchAll(client, 10L, searchRequest);
//        for (SearchHit hit : searchHits) {
//            System.out.println("search -> " + hit.getSourceAsString());
//        }
//        System.out.println(searchHits.size());
        SearchResponse searchResponse = client.search(searchRequest);

        Terms terms1 = searchResponse.getAggregations().get("extra_id_dist");
        List<String> result = new ArrayList<>();
        System.out.println(terms1.getBuckets().size());
        for (Terms.Bucket bucket1:terms1.getBuckets()){
            Terms terms2 = bucket1.getAggregations().get("type_count");
            for (Terms.Bucket bucket2 : terms2.getBuckets()) {
                JSONObject jsonObj = new JSONObject();
//                System.out.println("key:" + bucket1.getKey().toString() + "###" + bucket2.getKey().toString() + ";value:" + bucket2.getDocCount());
                Cardinality deviceIdCount = bucket2.getAggregations().get("device_id_count");
//                System.out.println("key:" + bucket1.getKey().toString() + "###" + deviceIdCount.getName()+ ";value:" + deviceIdCount.getValue());
                jsonObj.put("extra_id", bucket1.getKey().toString());
                jsonObj.put("type" , bucket2.getKey().toString());
                jsonObj.put("points" ,  bucket2.getDocCount());
                jsonObj.put("device_id_count",deviceIdCount.getValue());
                result.add(jsonObj.toJSONString());
            }
        }
        for (String v : result){
            System.out.println(v);
        }

        client.close();
    }


}
