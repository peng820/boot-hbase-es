package com.example.boothbasees.util;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.example.boothbasees.entity.EsIndex;
import com.example.boothbasees.entity.HbaseTest;
import com.example.boothbasees.response.PageResponse;
import com.example.boothbasees.template.HbaseTemplate;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * @Author: Lenovo
 * @Date: 2023/02/14/14:58
 * @Description:
 */
@Slf4j
@Component
public class EsUtil {

    @Resource
    RestHighLevelClient restHighLevelClient;

    @Resource
    private ElasticsearchOperations elasticsearchOperations;

    @Resource
    HbaseTemplate hbaseTemplate;

    /**
     * 前缀
     */
    private static String indexPrefix = "index_";


    public static String generatorDwdMixDetailKey(String month) {
        return indexPrefix + month;
    }


    /**
     * 分页 Hbase 二级索引查询
     * @param pageNumber
     * @param pageSize
     * @param key
     * @param startTime
     * @param endTime
     * @return
     */
    public PageResponse<HbaseTest> getPage(Integer pageNumber, Integer pageSize, String key, String startTime, String endTime) {
        LocalDate date = LocalDate.now();
        String month = date.getMonthValue() < 10 ? "0" + date.getMonthValue() : String.valueOf(date.getMonthValue());
        IndexCoordinates indexCoordinates = IndexCoordinates.of(generatorDwdMixDetailKey(month));
        PageResponse<HbaseTest> page = new PageResponse<>();
        BoolQueryBuilder filter = QueryBuilders.boolQuery();
        if (StrUtil.isNotEmpty(key)) {
            filter.must(QueryBuilders.matchQuery("key", key));
        }
        if (StrUtil.isNotBlank(startTime) && StrUtil.isNotBlank(endTime)) {
            filter.must(
                    QueryBuilders.rangeQuery("receiveTime")
                            .gte(DateUtils.stringToTimeMillis(startTime, DateUtils.DETAIL_FORMAT_STRING))
                            .lte(DateUtils.stringToTimeMillis(endTime, DateUtils.DETAIL_FORMAT_STRING))
            );
        }

        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withFilter(filter);
        queryBuilder.withSort(SortBuilders.fieldSort("receiveTime").order(SortOrder.ASC));
        queryBuilder.withPageable(PageRequest.of(pageNumber - 1, pageSize));
        NativeSearchQuery nativeSearchQuery = queryBuilder.build();

        SearchHits<EsIndex> search = elasticsearchOperations.search(nativeSearchQuery, EsIndex.class, indexCoordinates);

        if (search.getTotalHits() > 0) {
            List<String> ids = search.stream().map(SearchHit::getContent).map(EsIndex::getId).collect(Collectors.toList());
            List<HbaseTest> list = hbaseTemplate.getList(HbaseTest.class, ids);
            page.setRecords(list);
            page.setCurrent(pageNumber);
            page.setSize(pageSize);
            page.setTotal(search.getTotalHits());
        }
        return page;
    }

    /**
     * 批量插入
     */
    public void saveBatch(List<String> list) {
        LocalDate date = LocalDate.now();
        String month = date.getMonthValue() < 10 ? "0" + date.getMonthValue() : String.valueOf(date.getMonthValue());
        BulkProcessor bulkProcessor = getBulkProcessor();
        List<IndexRequest> indexRequests = new ArrayList<>();
        list.forEach(e -> {
            String indexName = generatorDwdMixDetailKey(month);
            IndexRequest request = new IndexRequest(indexName);
            request.source(e, XContentType.JSON);
            request.opType(DocWriteRequest.OpType.CREATE);
            indexRequests.add(request);
        });
        indexRequests.forEach(bulkProcessor::add);
    }

    /**
     * 批量更新
     */
    public void updateBatch(List<String> list) {
        LocalDate date = LocalDate.now();
        String month = date.getMonthValue() < 10 ? "0" + date.getMonthValue() : String.valueOf(date.getMonthValue());
        BulkProcessor bulkProcessor = getBulkProcessor();
        List<IndexRequest> indexRequests = new ArrayList<>();
        list.forEach(e -> {
            JSONObject object = JSONObject.parseObject(e);
            String indexName = generatorDwdMixDetailKey(month);
            IndexRequest request = new IndexRequest(indexName);
            // 设置id,不设置id使用自动生成的id数据插入速度更快
            try {
                request.id(object.getString("id"));
            } catch (Exception ex) {
            }
            request.source(e, XContentType.JSON);
            request.opType(DocWriteRequest.OpType.UPDATE);
            indexRequests.add(request);
        });
        indexRequests.forEach(bulkProcessor::add);
    }

    private BulkProcessor getBulkProcessor() {
        BulkProcessor bulkProcessor = null;

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            /**
             * beforeBulk会在批量提交之前执行，可以从BulkRequest中获取请求信息request.requests()或者请求数量request.numberOfActions()。
             */
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            /**
             * 会在批量失败后执行。
             */
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                log.error("[es错误]---尝试插入数据失败---", failure);
            }

            /**
             * 会在批量成功后执行，可以跟beforeBulk配合计算批量所需时间。
             */
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                boolean hasFailures = response.hasFailures();
                if (hasFailures) {
                    String message = response.getItems()[0].getFailure().getCause().getMessage();
                    log.info("---尝试插入{}条数据失败---", request.numberOfActions());
                    log.info("---失败原因---" + message);
                }
            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (request, bulkListener) ->
                restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);
        //到达5000条时刷新
        builder.setBulkActions(5000);
        //内存到达10M时刷新
        builder.setBulkSize(new ByteSizeValue(10L, ByteSizeUnit.MB));
        //设置允许执行的并发请求数。
        builder.setConcurrentRequests(2);
        //设置的刷新间隔30s
        builder.setFlushInterval(TimeValue.timeValueSeconds(30L));
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(10L), 3));
        bulkProcessor = builder.build();

        return bulkProcessor;
    }
}
