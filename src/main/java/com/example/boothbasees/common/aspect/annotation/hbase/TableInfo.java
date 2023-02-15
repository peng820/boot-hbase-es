package com.example.boothbasees.common.aspect.annotation.hbase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Lenovo
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface TableInfo {

    /**
     * 列簇
     */
    String columnFamily() default "INFO";

    /**
     * 表
     * @return
     */
    String table() default "";

}
