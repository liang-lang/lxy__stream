package com.retailersv.dwd;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.bean.TableProcessDwd;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.JdbcUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

public class DbusDwdBaseDb2Kafka {
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 创建kafka数据源
        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_bootstrap_server,
                        ODS_KAFKA_TOPIC,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ), WatermarkStrategy.noWatermarks(), "read_kafka_realtime_db"
        );
//        kafkaSourceDs.print() ;

        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSourceDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(s);
                    collector.collect(jsonObj);
                } catch (Exception e) {
                    System.out.println("非标准JSON");
                }
            }
        });
        MySqlSource<String> mySQLCdcDwdConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "realtime_v1_config.table_process_dwd",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial(),
                "10000-10050"
        );
        DataStreamSource<String> cdcDbDwdStream = env.fromSource(mySQLCdcDwdConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dwd_source");

        SingleOutputStreamOperator<TableProcessDwd> tpDs = cdcDbDwdStream.map(new MapFunction<String, TableProcessDwd>() {
            @Override
            public TableProcessDwd map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                String op = jsonObj.getString("op");
                TableProcessDwd tp = null;
                if ("d".equals(op)){
                    tp = jsonObj.getObject("before", TableProcessDwd.class);
                }else {
                    tp = jsonObj.getObject("after", TableProcessDwd.class);
                }
                tp.setOp( op);
                return tp;
            }
        });
//        tpDs.print();

        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDs = tpDs.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDs = jsonObjDs.connect(broadcastDs);

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDs = connectDs.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                    private final Map<String, TableProcessDwd> configMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection connection = JdbcUtils.getMySQLConnection(
                                ConfigUtils.getString("mysql.url"),
                                ConfigUtils.getString("mysql.user"),
                                ConfigUtils.getString("mysql.pwd"));
                        String querySQL = "select * from realtime_v1_config.table_process_dwd";
                        List<TableProcessDwd> tableProcessDwds = JdbcUtils.queryList(connection, querySQL, TableProcessDwd.class, true);
                        for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
                            configMap.put(tableProcessDwd.getSourceTable(), tableProcessDwd);
                        }

                        connection.close();
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
//                        {"op":"r","after":{"category_level":3,"category_id":178,"create_time":1639440000000,"attr_name":"像素","id":73},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"realtime_v1","table":"base_attr_info"},"ts_ms":1755243556351}
                        String tableName = jsonObj.getJSONObject("source").getString("table");
                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                        TableProcessDwd tp = null;
                        if ((tp = broadcastState.get(tableName)) != null || (tp = configMap.get(tableName)) != null) {
                            JSONObject after = jsonObj.getJSONObject("after");
                            deleteNotNeedColumns(after, tp.getSinkColumns());
                            after.put("ts", jsonObj.getLongValue("ts_ms"));
                            collector.collect(Tuple2.of(after, tp));
                        }

                    }

                    @Override
                    public void processBroadcastElement(TableProcessDwd tp, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        String op = tp.getOp();
                        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);

                        if ("d".equals(op)) {
                            configMap.remove(tp.getSourceTable());
                            broadcastState.remove(tp.getSourceTable());
                        } else {
                            configMap.put(tp.getSourceTable(), tp);
                            broadcastState.put(tp.getSourceTable(), tp);
                        }

                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }
                }
        );
        splitDs.print();
        splitDs.sinkTo(KafkaUtils.getKafkaSinkDwd());


        env.execute();

    }

    private static void deleteNotNeedColumns(JSONObject jsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = jsonObj.entrySet();
        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));
    }
}
