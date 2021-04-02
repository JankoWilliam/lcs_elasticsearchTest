package cn.yintech.esUtil;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * ES集群线上环境（自建）配置
 */
public class ESConfig_20210304 {

    private static String hosts = "115.28.252.78"; // 集群地址，多个用,隔开
    private static int port = 9100; // 使用的端口号
    private static String schema = "http"; // 使用的协议
    private static ArrayList<HttpHost> hostList = null;

    private static int connectTimeOut = 1000; // 连接超时时间
    private static int socketTimeOut = 60000; // 连接超时时间
    private static int connectionRequestTimeOut = 500; // 获取连接的超时时间

    private static int maxConnectNum = 100; // 最大连接数
    private static int maxConnectPerRoute = 100; // 最大路由连接数

    private static CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    static {
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "upvo%@ajUW4R")); // 认证信息
        hostList = new ArrayList<HttpHost>();
        String[] hostStrs = hosts.split(",");
        for (String host : hostStrs) {
            hostList.add(new HttpHost(host, port, schema));
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        int num = 0;
        for (String i : searchOnlineAgg("62749", "2021-02-24 09:59:27", "2021-02-24 10:53:41")) {
            if (i.contains("圈子视频直播")) {
                JSONParser jp = new JSONParser();
                JSONObject parse = (JSONObject)jp.parse(i);
                num += 1;
//                System.out.println(i);
                System.out.println(parse.getOrDefault("device_id",""));
            }
        }

        System.out.println(num);

    }

    public static RestHighLevelClient client() {
        RestClientBuilder builder = RestClient.builder(hostList.toArray(new HttpHost[0]));
        // 异步httpclient连接延时配置
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public Builder customizeRequestConfig(Builder requestConfigBuilder) {
                requestConfigBuilder.setConnectTimeout(connectTimeOut);
                requestConfigBuilder.setSocketTimeout(socketTimeOut);
                requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeOut);
                return requestConfigBuilder;
            }
        });
        // 异步httpclient连接数配置
        builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.setMaxConnTotal(maxConnectNum);
                httpClientBuilder.setMaxConnPerRoute(maxConnectPerRoute);
                httpClientBuilder.disableAuthCaching();
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                return httpClientBuilder;
            }
        });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }


    /**
     * 使用游标获取全部结果，返回SearchHit集合
     *
     * @param restHighLevelClient
     * @param scrollTimeOut
     * @param searchRequest
     * @return
     * @throws IOException
     */
    public static List<SearchHit> scrollSearchAll(RestHighLevelClient restHighLevelClient, Long scrollTimeOut, SearchRequest searchRequest) throws IOException {
        Scroll scroll = new Scroll(timeValueMillis(scrollTimeOut));
        searchRequest.scroll(scroll);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<SearchHit> resultSearchHit = new ArrayList<>();
        while (ArrayUtils.isNotEmpty(hits)) {
            for (SearchHit hit : hits) {
                resultSearchHit.add(hit);
            }
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(scroll);
            SearchResponse searchScrollResponse = restHighLevelClient.searchScroll(searchScrollRequest);
            scrollId = searchScrollResponse.getScrollId();
            hits = searchScrollResponse.getHits().getHits();
        }
        //及时清除es快照，释放资源
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        restHighLevelClient.clearScroll(clearScrollRequest);
        return resultSearchHit;
    }


    public static List<String> searchOnlineAgg(String extraId, String startTime, String endTime) throws IOException {

        RestHighLevelClient client = client();
        // bool查询
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        // bool子查询：范围查询
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("logtime"); //新建range条件
        if (startTime.contains("now"))
            rangeQueryBuilder.gte(startTime);
        else
            rangeQueryBuilder.gte(startTime.replace(" ", "T") + "+08:00"); //开始时间
        if (endTime.contains("now"))
            rangeQueryBuilder.lte(endTime);
        else
            rangeQueryBuilder.lte(endTime.replace(" ", "T") + "+08:00"); //结束时间
        boolBuilder.must(rangeQueryBuilder);
        if (extraId.length() > 0) {
            boolBuilder.must(QueryBuilders.matchQuery("extra_id", extraId));
        }
//        boolBuilder.must(QueryBuilders.matchQuery("is_online", "1"));
        // bool子查询：字符匹配查询
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
        AggregationBuilder aggs1 = AggregationBuilders.terms("extra_id_dist").field("extra_id");
        AggregationBuilder aggs2 = AggregationBuilders.terms("type_count").field("type");
        AggregationBuilder aggs3 = AggregationBuilders.terms("device_id").field("device_id").size(1000000000);
        AggregationBuilder aggs4 = AggregationBuilders.terms("uid").field("uid").size(1000000000);
//        aggs1.subAggregation(aggs2);
        aggs1.subAggregation(aggs2.subAggregation(aggs3.subAggregation(aggs4)));
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
        JSONObject jsonObj = new JSONObject();
        for (Terms.Bucket bucket1 : terms1.getBuckets()) {
            Terms terms2 = bucket1.getAggregations().get("type_count");
            for (Terms.Bucket bucket2 : terms2.getBuckets()) {
//                System.out.println("key:" + bucket1.getKey().toString() + "###" + bucket2.getKey().toString() + ";value:" + bucket2.getDocCount());
                Terms terms3 = bucket2.getAggregations().get("device_id");
                for (Terms.Bucket bucket3 : terms3.getBuckets()) {
                    Terms terms4 = bucket3.getAggregations().get("uid");
                    for (Terms.Bucket bucket4 : terms4.getBuckets()) {
                        jsonObj.put("extra_id", bucket1.getKey().toString());
                        jsonObj.put("type", bucket2.getKey().toString());
                        jsonObj.put("device_id", bucket3.getKey().toString());
                        jsonObj.put("uid", bucket4.getKey().toString());
                        jsonObj.put("points", bucket4.getDocCount());
                        result.add(jsonObj.toJSONString());
                    }
                }
            }
        }

        client.close();
        return result;
    }

}