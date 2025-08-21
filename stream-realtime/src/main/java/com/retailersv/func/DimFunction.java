package com.retailersv.func;

import com.alibaba.fastjson.JSONObject;

public interface DimFunction<T> {
    String getRowKey(T bean);

    String getTableName();

    void addDims(T bean, JSONObject dim);
}
