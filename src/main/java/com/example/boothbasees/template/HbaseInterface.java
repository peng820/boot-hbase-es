package com.example.boothbasees.template;

import org.apache.hadoop.hbase.filter.FilterList;

import java.util.List;

/**
 * @Author: Lenovo
 * @Date: 2023/02/14/11:43
 * @Description:
 */
public interface HbaseInterface {

    /**
     * 创建表
     *
     * @param aClass
     * @param <T>
     */
    <T> void createTable(Class<T> aClass);

    /**
     * 创建表
     *
     * @param aClass
     * @param tableName
     * @param <T>
     */
    <T> void createTable(Class<T> aClass,String tableName);

    /**
     * 删除数据
     *
     * @param aClass
     * @param rowKey
     * @param <T>
     */
    <T> void delete(Class<T> aClass, String rowKey);

    /**
     * 删除数据
     *
     * @param aClass
     * @param rowKeys
     * @param <T>
     */
    <T> void delete(Class<T> aClass, List<String> rowKeys);

    /**
     * 删除数据
     *
     * @param tableName
     * @param rowKey
     * @param <T>
     */
    <T> void delete(String tableName, String rowKey);

    /**
     * 删除数据
     *
     * @param tableName
     * @param rowKeys
     * @param <T>
     */
    <T> void delete(String tableName, List<String> rowKeys);

    /**
     * 新增或修改
     *
     * @param data
     * @param <T>
     */
    <T> void insertOrUpdate(T data);

    /**
     * 单条查询
     *
     * @param aClass
     * @param rowKey
     * @param <T>
     * @return
     */
    <T> T getOne(Class<T> aClass, String rowKey);

    /**
     * 单条查询
     *
     * @param aClass
     * @param tableName
     * @param rowKey
     * @param <T>
     * @return
     */
    <T> T getOne(Class<T> aClass, String tableName, String rowKey);

    /**
     * 多条查询
     *
     * @param aClass
     * @param rowKeys
     * @param <T>
     * @return
     */
    <T> List<T> getList(Class<T> aClass, List<String> rowKeys);

    /**
     * 多条查询
     *
     * @param aClass
     * @param tableName
     * @param rowKeys
     * @param <T>
     * @return
     */
    <T> List<T> getList(Class<T> aClass, String tableName, List<String> rowKeys);

    /**
     * 自定义查询条件
     *
     * @param aClass
     * @param filterList
     * @param <T>
     * @return
     */
    <T> List<T> scanFilter(Class<T> aClass, FilterList filterList);

    /**
     * 自定义查询条件
     *
     * @param aClass
     * @param tableName
     * @param filterList
     * @param <T>
     * @return
     */
    <T> List<T> scanFilter(Class<T> aClass, String tableName, FilterList filterList);
}
