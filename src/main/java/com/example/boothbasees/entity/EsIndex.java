package com.example.boothbasees.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Date;

/**
 * @Author: Lenovo
 * @Date: 2023/02/14/16:06
 * @Description:
 */
@Data
public class EsIndex {

    @Id
    @Field(type = FieldType.Keyword)
    public String id;

    @Field(type = FieldType.Keyword)
    public String key;

    @Field(type = FieldType.Date)
    public Date receiveTime;

}
