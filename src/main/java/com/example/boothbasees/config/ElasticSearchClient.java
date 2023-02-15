package com.example.boothbasees.config;

import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lenovo
 * @Date: 2023/02/13/16:30
 * @Description:
 */
@Configuration
public class ElasticSearchClient extends AbstractElasticsearchConfiguration {

    @Value("${spring.elasticsearch.rest.uris}")
    private List<String> hosts;

    @SneakyThrows
    @Override
    @Bean
    public RestHighLevelClient elasticsearchClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        // 通过builder创建rest client，配置http client的HttpClientConfigCallback。
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        for (String host : hosts) {
            URL hostUrl = new URL(host);
            httpHosts.add(new HttpHost(hostUrl.getHost(), hostUrl.getPort(), hostUrl.getProtocol()));
        }
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]))
                .setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        //防止 java.net.SocketTimeoutException: 30,000 milliseconds timeout on connection http-outgoing-3
        builder.setRequestConfigCallback(requestConfigBuilder -> {
            return requestConfigBuilder
                    .setConnectTimeout(5000*1000)
                    .setSocketTimeout(6000 * 1000);//更改客户端的超时限制默认30秒现在改为6000s
        });

        // RestHighLevelClient实例通过REST low-level client builder进行构造。
        RestHighLevelClient highClient = new RestHighLevelClient(builder);
        return highClient;
    }
}
