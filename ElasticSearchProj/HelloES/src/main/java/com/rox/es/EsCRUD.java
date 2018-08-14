package com.rox.es;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;


/**
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.4/index.html
 */
public class EsCRUD {

    private TransportClient client = null;

    @Before
    public void init() throws Exception {
        //设置集群名称
        Settings settings = Settings.builder()
                .put("cluster.name", "my-es")
                //自动感知的功能（可以通过当前指定的节点获取所有es节点的信息）
                .put("client.transport.sniff", true)
                .build();
        //创建client
        client = new PreBuiltTransportClient(settings).addTransportAddresses(
                new InetSocketTransportAddress(InetAddress.getByName("192.168.170.170"), 9300),
                new InetSocketTransportAddress(InetAddress.getByName("192.168.170.171"), 9300),
                new InetSocketTransportAddress(InetAddress.getByName("192.168.170.172"), 9300));

    }


    /**
     * 新增
     * @throws IOException
     */
    @Test
    public void testCreate() throws IOException {

        IndexResponse response = client.prepareIndex("gamelog", "users", "1")
                .setSource(
                        jsonBuilder()
                                .startObject()
                                    .field("username", "老李")
                                    .field("gender", "male")
                                    .field("birthday", new Date())
                                    .field("fv", 999)
                                    .field("message", "老李还行")
                                .endObject()
                ).get();


    }

    //查找一条
    @Test
    public void testGet() throws IOException {
        GetResponse response = client.prepareGet("gamelog", "users", "1").get();
        System.out.println(response.getSourceAsString());
    }


    //查找多条
    @Test
    public void testMultiGet() throws IOException {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("gamelog", "users", "1")
                .add("gamelog", "users", "2", "3")
                .add("news", "fulltext", "1")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println(json);
            }
        }
    }

    /**
     * 更新 (改)
     * @throws Exception
     */
    @Test
    public void testUpdate() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("gamelog");
        updateRequest.type("users");
        updateRequest.id("2");
        updateRequest.doc(
                jsonBuilder()
                        .startObject()
                        .field("fv", 999.9)
                        .endObject());
        client.update(updateRequest).get();
    }

    /**
     * 删除
     */
    @Test
    public void testDelete() {
        DeleteResponse response = client.prepareDelete("gamelog", "users", "1").get();
        System.out.println(response);
    }


    /**
     * 查询出结果 -> 删除
     */
    @Test
    public void testDeleteByQuery() {
        BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        //指定查询条件
                        .filter(QueryBuilders.matchQuery("username", "老段"))
                        //指定索引名称
                        .source("gamelog")
                        .get();
        long deleted = response.getDeleted();
        System.out.println(deleted);
    }


    //异步删除
    @Test
    public void testDeleteByQueryAsync() {
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("gender", "male"))
                .source("gamelog")
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        long deleted = response.getDeleted();
                        System.out.println("数据删除了");
                        System.out.println(deleted);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        e.printStackTrace();
                    }
                });
        try {
            System.out.println("异步删除");
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 范围查询
     * 注意: 这里如果前面设置的是 doule 类型, 搜索的范围也是 double 类型
     */
    @Test
    public void testRange() {

        QueryBuilder qb = rangeQuery("fv")
                // [88.99, 10000)
                .from(88.99)
                .to(100000)
                .includeLower(true)
                .includeUpper(false);

        SearchResponse response = client.prepareSearch("gamelog").setQuery(qb).get();

        System.out.println(response);
    }


    /**
     * NBA相关查询
     * curl -XPUT 'http://192.168.5.251:9200/player_info/player/1' -d '{ "name": "curry", "age": 29, "salary": 3500,"team": "war", "position": "pg"}'
     * curl -XPUT 'http://192.168.5.251:9200/player_info/player/2' -d '{ "name": "thompson", "age": 26, "salary": 2000,"team": "war", "position": "pg"}'
     * curl -XPUT 'http://192.168.5.251:9200/player_info/player/3' -d '{ "name": "irving", "age": 25, "salary": 2000,"team": "cav", "position": "pg"}'
     * curl -XPUT 'http://192.168.5.251:9200/player_info/player/4' -d '{ "name": "green", "age": 26, "salary": 2000,"team": "war", "position": "pf"}'
     * curl -XPUT 'http://192.168.5.251:9200/player_info/player/5' -d '{ "name": "james", "age": 33, "salary": 4000,"team": "cav", "position": "sf"}'
     */

    // 创建 index 和 type
    @Test
    public void testAddPlayer() throws IOException {

        IndexResponse response = client.prepareIndex("player_info", "player", "1")
                .setSource(
                        jsonBuilder()
                                .startObject()
                                .field("name", "James")
                                .field("age", 33)
                                .field("salary", 3000)
                                .field("team", "cav")
                                .field("position", "sf")
                                .endObject()
                ).get();
    }

    /**
     * 复杂条件查询
     * https://elasticsearch.cn/article/102
     *
     * select team, count(*) as player_count from player group by team;
     */
    @Test
    public void testAgg1() {

        //指定索引和type
        SearchRequestBuilder builder = client.prepareSearch("player_info").setTypes("player");
        //按team分组然后聚合，但是并没有指定聚合函数
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("player_count").field("team");
        //添加聚合器
        builder.addAggregation(teamAgg);
        //触发
        SearchResponse response = builder.execute().actionGet();
        //System.out.println(response);
        //将返回的结果放入到一个map中
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
//        Set<String> keys = aggMap.keySet();
//
//        for (String key: keys) {
//            System.out.println(key);
//        }

//        //取出聚合属性
        StringTerms terms = (StringTerms) aggMap.get("player_count");


        //
////        //依次迭代出分组聚合数据
//        for (Terms.Bucket bucket : terms.getBuckets()) {
//            //分组的名字
//            String team = (String) bucket.getKey();
//            //count，分组后一个组有多少数据
//            long count = bucket.getDocCount();
//            System.out.println(team + " " + count);
//        }

        Iterator<Terms.Bucket> teamBucketIt = terms.getBuckets().iterator();
        while (teamBucketIt .hasNext()) {
            Terms.Bucket bucket = teamBucketIt.next();
            String team = (String) bucket.getKey();

            long count = bucket.getDocCount();

            System.out.println(team + " " + count);
        }

    }

    /**
     * select team, position, count(*) as pos_count from player group by team, position;
     * 先按 team, team 中再按 position 聚合
     */
    @Test
    public void testAgg2() {
        SearchRequestBuilder builder = client.prepareSearch("player_info").setTypes("player");
        //指定别名和分组的字段
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team_name").field("team");
        TermsAggregationBuilder posAgg= AggregationBuilders.terms("pos_count").field("position");
        //添加两个聚合构建器
        /**
         * 注意: 此处是在 teamAgg 上, 添加 subAggregation(posAgg)
         */
        builder.addAggregation(teamAgg.subAggregation(posAgg));
        //执行查询
        SearchResponse response = builder.execute().actionGet();
        //将查询结果放入map中
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        //根据属性名到map中查找
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        //循环查找结果
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            //先按球队进行分组
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            StringTerms positions = (StringTerms) subAggMap.get("pos_count");
            //因为一个球队有很多位置，那么还要依次拿出位置信息
            for (Terms.Bucket posBucket : positions.getBuckets()) {
                //拿到位置的名字
                String pos = (String) posBucket.getKey();
                //拿出该位置的数量
                long docCount = posBucket.getDocCount();
                //打印球队，位置，人数
                System.out.println(team + " " + pos + " " + docCount);
            }
        }

        /**
         * 结果:
         war pg 2
         war pf 1
         cav pg 1
         cav sf 1
         */
    }


    /**
     * select team, max(age) as max_age from player group by team;
     * 根据 team 名聚合后, 求聚合成员的 max age
     */
    @Test
    public void testAgg3() {

        // 指定 索引&类型  index & type
        SearchRequestBuilder builder = client.prepareSearch("player_info").setTypes("player");
        /**
         * term: 根据指定的名字创建一个新的聚合体
         * 指定按球队进行分组
         */
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team_name").field("team");

        //指定分组求最大值, 指定聚合函数(max)和需要聚合的字段(age)
        MaxAggregationBuilder maxAgg = AggregationBuilders.max("max_age").field("age");

        //分组后求最大值(添加聚合器--聚合规则)
        builder.addAggregation(teamAgg.subAggregation(maxAgg));

        //查询
        SearchResponse response = builder.execute().actionGet();

        // 以 map 的形式获取聚合结果
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();

        //根据team属性为 key，获取map中的内容-value
        StringTerms teams = (StringTerms) aggMap.get("team_name");

        for (Terms.Bucket teamBucket : teams.getBuckets()) {

            //分组的属性名--有很多对, 把队名取出来
            String team = (String) teamBucket.getKey();

            //在将聚合后取最大值的内容取出来放到map中
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();

            //取分组后的最大值
            InternalMax ages = (InternalMax)subAggMap.get("max_age");
            double max = ages.getValue();
            System.out.println(team + " " + max);
        }
    }


    /**
     * select team, avg(age) as avg_age, sum(salary) as total_salary from player group by team;
     * team分组 下求 avg & sum
     */
    @Test
    public void testAgg4() {
        SearchRequestBuilder builder = client.prepareSearch("player_info").setTypes("player");
        //指定分组字段
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("team_name").field("team");
        //指定聚合函数是求平均数据
        AvgAggregationBuilder avgAgg = AggregationBuilders.avg("avg_age").field("age");
        //指定另外一个聚合函数是求和
        SumAggregationBuilder sumAgg = AggregationBuilders.sum("total_salary").field("salary");

        //分组的聚合器关联了两个聚合函数(team 下关联了 avg & sum)
        builder.addAggregation(termsAgg.subAggregation(avgAgg).subAggregation(sumAgg));

        SearchResponse response = builder.execute().actionGet();

        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();

        //按分组的名字取出数据
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            //获取球队名字
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            //根据别名取出平均年龄
            InternalAvg avgAge = (InternalAvg)subAggMap.get("avg_age");
            //根据别名取出薪水总和
            InternalSum totalSalary = (InternalSum)subAggMap.get("total_salary");
            double avgAgeValue = avgAge.getValue();
            double totalSalaryValue = totalSalary.getValue();
            System.out.println(team + " " + avgAgeValue + " " + totalSalaryValue);
        }
    }


    /**
     * select team, sum(salary) as total_salary from player group by team order by total_salary desc;
     * 按 team 聚合, 按sum salary 降序排列
     */
    @Test
    public void testAgg5() {
        SearchRequestBuilder builder = client.prepareSearch("player_info").setTypes("player");
        //按team进行分组，然后指定排序规则  asc 升序, desc 降序
        //排序规则在分组时就指定了
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("team_name").field("team").order(Terms.Order.aggregation("total_salary ", false));
        SumAggregationBuilder sumAgg = AggregationBuilders.sum("total_salary").field("salary");
        builder.addAggregation(termsAgg.subAggregation(sumAgg));
        SearchResponse response = builder.execute().actionGet();
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            InternalSum totalSalary = (InternalSum)subAggMap.get("total_salary");
            double totalSalaryValue = totalSalary.getValue();
            System.out.println(team + " " + totalSalaryValue);
        }
    }
}
