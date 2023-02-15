package com.example.boothbasees.template;

import cn.hutool.core.util.ObjectUtil;
import com.example.boothbasees.common.aspect.annotation.hbase.ColumnGroup;
import com.example.boothbasees.common.aspect.annotation.hbase.RowKey;
import com.example.boothbasees.common.aspect.annotation.hbase.TableInfo;
import com.example.boothbasees.common.exception.HbaseCustomException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.CaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lenovo
 * @Date: 2023/02/14/12:01
 * @Description:
 */
@Component
public class HbaseTemplate implements HbaseInterface {
    @Resource
    private Connection hbaseConnection;

    @Override
    public <T> void createTable(Class<T> aClass) {
        createTable(aClass, "");
    }

    @Override
    public <T> void createTable(Class<T> aClass, String tableName) {
        Admin admin = null;
        TableInfo annotation = aClass.getAnnotation(TableInfo.class);
        try {
            String name = tableName.isEmpty() ? annotation.table() : annotation.table() + tableName;
            admin = hbaseConnection.getAdmin();
            TableName table = TableName.valueOf(name);
            if (!admin.isTableAvailable(table)) {
                TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(table);
                // 构建列族对象
                ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(annotation.columnFamily())).build();
                tableDescriptor.setColumnFamily(family);
                // 创建表
                admin.createTable(tableDescriptor.build());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T> void delete(Class<T> aClass, String rowKey) {
        TableInfo annotation = aClass.getAnnotation(TableInfo.class);
        delete(annotation.table(), rowKey);
    }

    @Override
    public <T> void delete(Class<T> aClass, List<String> rowKeys) {
        TableInfo annotation = aClass.getAnnotation(TableInfo.class);
        delete(annotation.table(), rowKeys);
    }

    @Override
    public <T> void delete(String tableName, String rowKey) {
        try {
            TableName table = TableName.valueOf(tableName);
            Table t = hbaseConnection.getTable(table);
            Delete del = new Delete(Bytes.toBytes(rowKey));
            t.delete(del);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T> void delete(String tableName, List<String> rowKeys) {
        try {
            TableName table = TableName.valueOf(tableName);
            Table t = hbaseConnection.getTable(table);
            List<Delete> deletes = new ArrayList<>();
            for (String rowKey : rowKeys) {
                deletes.add(new Delete(Bytes.toBytes(rowKey)));
            }
            t.delete(deletes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T> void insertOrUpdate(T data) {
        try {
            // 新增数据
            Class<?> aClass = data.getClass();
            TableInfo annotation = aClass.getAnnotation(TableInfo.class);
            Table table = hbaseConnection.getTable(TableName.valueOf(annotation.table()));
            Put put = null;
            Field[] declaredFields = aClass.getDeclaredFields();
            String methodName;
            try {
                for (Field field : declaredFields) {
                    if (field.getAnnotation(RowKey.class) != null || field.getAnnotation(ColumnGroup.class) != null) {
                        String columnName = field.getName();
                        methodName = "get" + columnName.substring(0, 1).toUpperCase() + columnName.substring(1);
                        Method method = data.getClass().getMethod(methodName);
                        Object invoke = method.invoke(data);
                        if (ObjectUtil.isNotEmpty(invoke)) {
                            if (field.getAnnotation(RowKey.class) != null) {
                                String rowKeyValue = invoke.toString();
                                if (StringUtils.isEmpty(rowKeyValue)) {
                                    throw new HbaseCustomException("RowKey is null");
                                }
                                put = new Put(rowKeyValue.getBytes());
                            } else if (field.getAnnotation(ColumnGroup.class) != null) {
                                ColumnGroup group = field.getAnnotation(ColumnGroup.class);
                                put.addColumn(annotation.columnFamily().getBytes(), group.value().getBytes(), invoke.toString().getBytes());
                            }
                        }
                    }
                }
                if (put != null) {
                    table.put(put);
                } else {
                    throw new HbaseCustomException("entity lack RowKey");
                }
            } catch (HbaseCustomException e) {
                throw e;
            } finally {
                if (table != null) {
                    table.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T> T getOne(Class<T> aClass, String rowKey) {
        return getOne(aClass, "", rowKey);
    }

    @Override
    public <T> T getOne(Class<T> aClass, String tableName, String rowKey) {
        T entity = null;
        try {
            entity = aClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        TableInfo annotation = aClass.getAnnotation(TableInfo.class);
        String name = tableName.isEmpty() ? annotation.table() : annotation.table() + tableName;
        Field[] declaredFields = aClass.getDeclaredFields();
        try (Table table = hbaseConnection.getTable(TableName.valueOf(name))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            String setMethodeKey = "";
            for (Field field : declaredFields) {
                if (field.getAnnotation(RowKey.class) != null) {
                    RowKey key = field.getAnnotation(RowKey.class);
                    setMethodeKey = "set" + CaseUtils.toCamelCase(key.value().toLowerCase(), true, new char[]{'_'});
                } else if (field.getAnnotation(ColumnGroup.class) != null) {
                    ColumnGroup group = field.getAnnotation(ColumnGroup.class);
                    get.addColumn(annotation.columnFamily().getBytes(), Bytes.toBytes(group.value()));
                }
            }
            Result result = table.get(get);
            String columnName;
            String methodName;
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                if (StringUtils.isNotEmpty(setMethodeKey)) {
                    String key = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    Method method = aClass.getMethod(setMethodeKey, String.class);
                    method.invoke(entity, key);
                }
                columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                Field declaredField = null;
                try {
                    String field = CaseUtils.toCamelCase(columnName.toLowerCase(), false, new char[]{'_'});
                    declaredField = aClass.getDeclaredField(field);
                } catch (Exception e) {
                }
                if (declaredField != null) {
                    methodName = "set" + CaseUtils.toCamelCase(columnName.toLowerCase(), true, new char[]{'_'});
                    Method method = aClass.getMethod(methodName, String.class);
                    method.invoke(entity, Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entity;
    }

    @Override
    public <T> List<T> getList(Class<T> aClass, List<String> rowKeys) {
        return getList(aClass, "", rowKeys);
    }

    @Override
    public <T> List<T> getList(Class<T> aClass, String tableName, List<String> rowKeys) {
        List<T> list = new ArrayList<>();
        TableInfo annotation = aClass.getAnnotation(TableInfo.class);
        String name = tableName.isEmpty() ? annotation.table() : annotation.table() + tableName;
        Field[] declaredFields = aClass.getDeclaredFields();
        try (Table table = hbaseConnection.getTable(TableName.valueOf(name))) {
            List<Get> gets = new ArrayList<>();
            for (String rowKey : rowKeys) {
                gets.add(new Get(Bytes.toBytes(rowKey)));
            }
            String setMethodeKey = "";
            for (Field field : declaredFields) {
                if (field.getAnnotation(RowKey.class) != null) {
                    RowKey key = field.getAnnotation(RowKey.class);
                    setMethodeKey = "set" + CaseUtils.toCamelCase(key.value().toLowerCase(), true, new char[]{'_'});
                }
            }
            Result[] results = table.get(gets);
            String columnName;
            String methodName;
            for (Result result : results) {
                T hbaseTest = aClass.newInstance();
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    if (StringUtils.isNotEmpty(setMethodeKey)) {
                        String rowkey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                        Method method = aClass.getMethod(setMethodeKey, String.class);
                        method.invoke(hbaseTest, rowkey);
                    }
                    columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    Field declaredField = null;
                    try {
                        String field = CaseUtils.toCamelCase(columnName.toLowerCase(), false, new char[]{'_'});
                        declaredField = aClass.getDeclaredField(field);
                    } catch (Exception e) {
                    }
                    if (declaredField != null) {
                        methodName = "set" + CaseUtils.toCamelCase(columnName.toLowerCase(), true, new char[]{'_'});
                        Method method = aClass.getMethod(methodName, String.class);
                        method.invoke(hbaseTest, Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
                list.add(hbaseTest);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    @Override
    public <T> List<T> scanFilter(Class<T> aClass, FilterList filterList) {
        return scanFilter(aClass, "", filterList);
    }

    @Override
    public <T> List<T> scanFilter(Class<T> aClass, String tableName, FilterList filterList) {
        List<T> list = new ArrayList<>();
        Table table = null;
        ResultScanner scanner = null;
        try {
            TableInfo annotation = aClass.getAnnotation(TableInfo.class);
            Field[] declaredFields = aClass.getDeclaredFields();
            table = hbaseConnection.getTable(TableName.valueOf(annotation.table() + tableName));
            Scan scan = new Scan();
            scan.setFilter(filterList);
            String setMethodeKey = "";
            for (Field field : declaredFields) {
                if (field.getAnnotation(RowKey.class) != null) {
                    RowKey key = field.getAnnotation(RowKey.class);
                    setMethodeKey = "set" + CaseUtils.toCamelCase(key.value().toLowerCase(), true, new char[]{'_'});
                } else if (field.getAnnotation(ColumnGroup.class) != null) {
                    ColumnGroup group = field.getAnnotation(ColumnGroup.class);
                    scan.addColumn(annotation.columnFamily().getBytes(), Bytes.toBytes(group.value()));
                }
            }
            scanner = table.getScanner(scan);
            String columnName = "";
            String methodName = "";
            for (Result result : scanner) {
                List<Cell> cells = result.listCells();
                boolean flag = true;
                T hbaseTest = aClass.newInstance();
                list.add(hbaseTest);
                for (Cell cell : cells) {
                    if (flag && StringUtils.isNotEmpty(setMethodeKey)) {
                        String rowkey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                        Method method = aClass.getMethod(setMethodeKey, String.class);
                        method.invoke(hbaseTest, rowkey);
                        flag = false;
                    }
                    columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    Field declaredField = null;
                    try {
                        String field = CaseUtils.toCamelCase(columnName.toLowerCase(), false, new char[]{'_'});
                        declaredField = aClass.getDeclaredField(field);
                    } catch (Exception e) {
                    }
                    if (declaredField != null) {
                        methodName = "set" + CaseUtils.toCamelCase(columnName.toLowerCase(), true, new char[]{'_'});
                        Method method = aClass.getMethod(methodName, String.class);
                        method.invoke(hbaseTest, Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
            }
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return list;
    }
}
