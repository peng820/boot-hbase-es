package com.example.boothbasees.entity;

import com.example.boothbasees.common.aspect.annotation.hbase.ColumnGroup;
import com.example.boothbasees.common.aspect.annotation.hbase.TableInfo;
import lombok.Data;

@Data
@TableInfo(table = "HSMB:DWD_MIX_DETAIL_2022")
public class HbaseTest {

    @ColumnGroup(value = "MMSI")
    private String mmsi;

}
