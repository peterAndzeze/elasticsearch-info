package net.sw.search.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * es 配置信息
 * @author sw
 */
@Configuration
public class EsConfig {
    @Value("${spring.elasticsearch.rest.uris}")
    private String hostList;

    public String getHostList() {
        return hostList;
    }

    public void setHostList(String hostList) {
        this.hostList = hostList;
    }

    /**
     * 创建es client
     * @return
     */
    @Bean
    public RestHighLevelClient client(){
        String [] split=hostList.split(",");
        int hostLength=split.length;
        HttpHost [] httpHostArr=new HttpHost[hostLength];

        for (int i = 0; i <hostLength ; i++) {
            String[] item=split[i].split(":");
            httpHostArr[i]=new HttpHost(item[0],Integer.parseInt(item[1]),"http");
        }
        /**
         * 创建实例
         */
        return  new RestHighLevelClient(RestClient.builder(httpHostArr));
    }
}
