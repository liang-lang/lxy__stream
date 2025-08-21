package com.retailersv.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;


public class DbusDwdTradeRefundPaySucDetail2Kafka {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_TRADE_REFUND_PAYMENT_SUCCESS = ConfigUtils.getString("kafka.dwd.trade.refund.pay.suc.detail");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));


        tableEnv.executeSql("CREATE TABLE ods_ecommerce_order (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()," +
                " time_ltz AS TO_TIMESTAMP_LTZ(ts_ms, 3),\n" +
                " WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_dwd_trade_order_refund"));

        // 从 hbase 中读取 字典数据 创建 字典表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")" + SqlUtil.getHbaseDDL("dim_base_dic"));

        // 3. 过滤退款成功表数据
        Table refundPayment = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['order_id'] order_id," +
                        "`after`['sku_id'] sku_id," +
                        "MD5(CAST(`after`['payment_type'] AS STRING)) payment_type,\n" +
                        "`after`['callback_time'] callback_time," +
                        "`after`['total_amount'] total_amount," +
                        "proc_time, " +
                        "ts_ms " +
                        "from ods_ecommerce_order " +
                        "where  `source`['table'] = 'refund_payment' " +
                        "and `op` = 'u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='1602'");
        tableEnv.createTemporaryView("refund_payment", refundPayment);

//        refundPayment.execute().print();

        // 4. 过滤退单表中的退单成功的数据
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['order_id'] order_id," +
                        "`after`['sku_id'] sku_id," +
                        "`after`['refund_num'] refund_num " +
                        "from ods_ecommerce_order " +
                        "where  `source`['table'] ='order_refund_info' " +
                        "and `op`='u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='0705'");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

//        orderRefundInfo.execute().print();

        // 5. 过滤订单表中的退款成功的数据
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['user_id'] user_id," +
                        "`after`['province_id'] province_id " +
                        "from ods_ecommerce_order " +
                        "where `source`['table'] ='order_info' " +
                        "and `op`='u' " +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1006'");
        tableEnv.createTemporaryView("order_info", orderInfo);

//        orderInfo.execute().print();

        // 6. 4 张表的 join
        Table result = tableEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "date_format(FROM_UNIXTIME(cast(rp.callback_time as bigint) / 1000), 'yyyy-MM-dd') date_id," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount," +
                        "rp.ts_ms " +
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.proc_time as dic " +
                        "on rp.payment_type=dic.dic_code ");

//        result.execute().print();

        // 7.写出到 kafka
        tableEnv.executeSql("create table " + DWD_TRADE_REFUND_PAYMENT_SUCCESS + "(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint, " +
                "primary key(id) not enforced " +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_REFUND_PAYMENT_SUCCESS));
        result.executeInsert(DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }
}
