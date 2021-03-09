package net.sw.search.service.impl;

import net.sw.search.helper.DBHepler;
import net.sw.search.service.PositionService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * 职位数据操作
 */
@Service
public class PositionServiceImpl implements PositionService {

    private static final Logger logger = LoggerFactory.getLogger(PositionServiceImpl.class);
    @Autowired
    private RestHighLevelClient restHighLevelClient;
    /**
     * 表对应的es索引
     */
    private static final String POSITION_INDEX = "position";

    @Override
    public List<Map<String, Object>> searchs(String keyWord, int pageNo, int pageSize) throws IOException {
        if (pageNo <= 1) {
            pageNo = 1;
        }
        SearchRequest searchRequest = new SearchRequest(POSITION_INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        /**
         * 分页数据
         */
        searchSourceBuilder.from((pageNo - 1) * pageSize);
        searchSourceBuilder.size(pageSize);

        /**
         * 精确匹配

         TermQueryBuilder termQueryBuilder= QueryBuilders.termQuery("positionName",keyWord);
         searchSourceBuilder.query(termQueryBuilder);
         */

        //QueryBuilder queryBuilder = QueryBuilders.matchQuery("positionAdvantage", keyWord);
        //多字段匹配查询
        //查询字段数组：
        String [] fieldNames={"positionAdvantage","companyName","city","workAddress"};
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(keyWord, fieldNames);
        searchSourceBuilder.query(multiMatchQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        /**
         * 执行搜索
         */
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit searchHit : hits) {
            list.add(searchHit.getSourceAsMap());
        }
        return list;
    }

    @Override
    public void importAll() {
        writeMysqlDataToES(POSITION_INDEX);
    }

    /**
     * 数据 批量写入mysql
     *
     * @param tableName
     */
    private void writeMysqlDataToES(String tableName) {
        BulkProcessor bulkProcessor = getBulkProcessor(restHighLevelClient);
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = DBHepler.getConn();
            String sql = "select * from " + tableName;
            ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(20);
            rs = ps.executeQuery();
            ResultSetMetaData colData = rs.getMetaData();
            ArrayList<HashMap<String, String>> dataList = new ArrayList<>();
            // bulkProcessor 添加的数据支持的方式并不多，查看其api发现其支持map键值对的 方式，故笔者在此将查出来的数据转换成hashMap方式
            HashMap<String, String> map = null;
            int count = 0;
            String c = null;
            String v = null;
            while (rs.next()) {
                count++;
                map = new HashMap<String, String>(128);
                for (int i = 1; i <= colData.getColumnCount(); i++) {
                    c = colData.getColumnName(i);
                    v = rs.getString(c);
                    map.put(c, v);
                }
                dataList.add(map);
                // 每1万条写一次，不足的批次的最后再一并提交
                if (count % 10000 == 0) {
                    logger.info("Mysql handle data number : " + count);
                    // 将数据添加到 bulkProcessor 中
                    for (HashMap<String, String> hashMap2 : dataList) {
                        bulkProcessor.add(new IndexRequest(POSITION_INDEX).source(hashMap2));
                    }
                    // 每提交一次便将map与list清空
                    map.clear();
                    dataList.clear();
                }
            }
            // 处理未提交的数据
            for (HashMap<String, String> hashMap2 : dataList) {
                bulkProcessor.add(new IndexRequest(POSITION_INDEX).source(hashMap2));
                System.out.println(hashMap2);
            }
            logger.info("-------------------------- Finally insert number total : " + count);
            // 将数据刷新到es, 注意这一步执行后并不会立即生效，取决于bulkProcessor设置的 刷新时间
            bulkProcessor.flush();

        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            try {
                rs.close();
                ps.close();
                connection.close();
                boolean terminatedFlag = bulkProcessor.awaitClose(150L, TimeUnit.SECONDS);
                logger.info(String.valueOf(terminatedFlag));
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

    private BulkProcessor getBulkProcessor(RestHighLevelClient restHighLevelClient) {
        BulkProcessor bulkProcessor = null;
        try {
            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    logger.info("Try to insert data number : " + request.numberOfActions());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    logger.info("************** Success insert data number : " + request.numberOfActions() + " , id: " + executionId);
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    logger.error("Bulk is unsuccess : " + failure + ", executionId: " + executionId);
                }
            };
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (request, bulkListener) -> restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
            BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);
            builder.setBulkActions(5000);
            builder.setBulkSize(new ByteSizeValue(100L, ByteSizeUnit.MB));
            builder.setConcurrentRequests(10);
            builder.setFlushInterval(TimeValue.timeValueSeconds(100L));
            builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
            // 注意点：让参数设置生效
            bulkProcessor = builder.build();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                bulkProcessor.awaitClose(100L, TimeUnit.SECONDS);
            } catch (Exception e1) {
                logger.error(e1.getMessage());
            }
        } return bulkProcessor;
    }
}
