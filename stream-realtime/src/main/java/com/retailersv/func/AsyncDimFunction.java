package com.retailersv.func;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;


@Slf4j
public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements DimFunction<T> {
    private static final String HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");


    private AsyncConnection hBaseAsyncConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeAsyncHbaseConnection(hBaseAsyncConn);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        JSONObject jsonObject = HBaseUtil.readDimAsync(hBaseAsyncConn,
                HBASE_NAME_SPACE, //namespace
                getTableName(),  // 表名
                getRowKey(input));// rowkey 主键

        addDims(input, jsonObject);
        resultFuture.complete(Collections.singleton(input));
    }

}
