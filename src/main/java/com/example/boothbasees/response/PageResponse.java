package com.example.boothbasees.response;

import lombok.Data;

import java.util.List;

/**
 * @Author: Lenovo
 * @Date: 2023/02/15/11:12
 * @Description:
 */
@Data
public class PageResponse<T> {

    protected List<T> records;
    protected long total;
    protected long size;
    protected long current;
}
