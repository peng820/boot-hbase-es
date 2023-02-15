package com.example.boothbasees;

import com.example.boothbasees.entity.HbaseTest;
import com.example.boothbasees.template.HbaseTemplate;
import com.example.boothbasees.util.EsUtil;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class BootHbaseHiveEsApplicationTests {

    @Resource
    HbaseTemplate hbaseTemplate;

    @Resource
    EsUtil esUtil;

    @Test
    void hbaseTest() {
        HbaseTest one = hbaseTemplate.getOne(HbaseTest.class, "100043200-20221212231201-1");
        System.out.println(one);
    }

    @Test
    void esTest(){
        String str = "{\"receiveTime\":1670382010370,\"key\":\"7237499020000554816\",\"id\":\"14d7cb180e76f3f65b9a2e6a543679ee\"}";
        List<String> list = new ArrayList<>();
        list.add(str);
        esUtil.saveBatch(list);
    }

    @Test
    void esPageTest(){
        esUtil.getPage(1,10,"7237499020000554816","2023-01-01 00:00:00","2023-01-31 23:59:59");
    }
}
