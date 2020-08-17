package cn.yintech.esUtil;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class EsHighRrestClientTest {

    public static void main(String[] args) throws IOException, InterruptedException {

        RestHighLevelClient client = ESConfig.client();
        // bool查询
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        // bool子查询：范围查询
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("@timestamp"); //新建range条件
        rangeQueryBuilder.gte("now-3m"); //开始时间，时间范围
//        SimpleDateFormat  sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
//        sdf.setTimeZone(TimeZone.getTimeZone("UTC-0"));
//        rangeQueryBuilder.gte(sdf.format(new Date())+"Z"); //开始时间
//        Thread.sleep(10000);
//        rangeQueryBuilder.lt(sdf.format(new Date())+"Z"); //结束时间
        boolBuilder.must(rangeQueryBuilder);
        // bool子查询：字符匹配查询
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("action", "app/balaComment");//这里可以根据字段进行搜索，must表示符合条件的，相反的mustnot表示不符合条件的
        boolBuilder.must(matchQueryBuilder);

        // 结果排序
//        SortBuilder sortBuilder = SortBuilders.fieldSort("@timestamp").order(SortOrder.ASC);// 排训规则
        // 新建查询源
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolBuilder); //设置查询，可以是任何类型的QueryBuilder。
        sourceBuilder.from(0); //设置确定结果要从哪个索引开始搜索的from选项，默认为0
        sourceBuilder.size(10000); //设置确定搜素命中返回数的size选项，默认为10
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); //设置一个可选的超时，控制允许搜索的时间。
//        sourceBuilder.sort(sortBuilder);
        // 第一个是获取字段，第二个是过滤的字段，默认获取全部
        //sourceBuilder.fetchSource(new String[] {"fields.port","fields.entity_id","fields.message"}, new String[] {});
        /**
         * 制定索引库和类型表
         */
//        SearchRequest searchRequest = new SearchRequest("real_time_count"); //索引：实时库
//        searchRequest.types("real_time_count"); //类型:实时表

        SearchRequest searchRequest = new SearchRequest("pro-yii2-elk-success"); //索引：接口库——success
        searchRequest.types("doc"); //类型:接口请求记录
        searchRequest.source(sourceBuilder);
        // 深度查询：分页查询全量数据
        List<SearchHit> searchHits = ESConfig.scrollSearchAll(client, 10L, searchRequest);

//        SearchResponse response = client.search(searchRequest);
//        SearchHits hits = response.getHits();  //SearchHits提供有关所有匹配的全局信息，例如总命中数或最高分数：
//        SearchHit[] searchHits = hits.getHits();
//
//        System.out.println("total hits : " + hits.totalHits);
//        System.out.println("total hits : " + searchHits.length);
        for (SearchHit hit : searchHits) {
            System.out.println("search -> " + hit.getSourceAsString());
        }
        System.out.println(searchHits.size());

        client.close();
    }


}
