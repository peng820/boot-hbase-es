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
     * ??????
     */
    private static String indexPrefix = "index_";


    public static String generatorDwdMixDetailKey(String month) {
        return indexPrefix + month;
    }


    /**
     * ?????? Hbase ??????????????????
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
     * ????????????
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
     * ????????????
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
            // ??????id,?????????id?????????????????????id????????????????????????
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
             * beforeBulk??????????????????????????????????????????BulkRequest?????????????????????request.requests()??????????????????request.numberOfActions()???
             */
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            /**
             * ??????????????????????????????
             */
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                log.error("[es??????]---????????????????????????---", failure);
            }

            /**
             * ???????????????????????????????????????beforeBulk?????????????????????????????????
             */
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                boolean hasFailures = response.hasFailures();
                if (hasFailures) {
                    String message = response.getItems()[0].getFailure().getCause().getMessage();
                    log.info("---????????????{}???????????????---", request.numberOfActions());
                    log.info("---????????????---" + message);
                }
            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (request, bulkListener) ->
                restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);
        //??????5000????????????
        builder.setBulkActions(5000);
        //????????????10M?????????
        builder.setBulkSize(new ByteSizeValue(10L, ByteSizeUnit.MB));
        //???????????????????????????????????????
        builder.setConcurrentRequests(2);
        //?????????????????????30s
        builder.setFlushInterval(TimeValue.timeValueSeconds(30L));
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(10L), 3));
        bulkProcessor = builder.build();

        return bulkProcessor;
    }
}
