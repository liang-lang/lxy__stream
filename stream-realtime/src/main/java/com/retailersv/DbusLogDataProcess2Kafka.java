package com.retailersv;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.*;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Date;
import java.util.HashMap;

public class DbusLogDataProcess2Kafka {
    private static final String kafka_topic_base_log_data = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_err_log = ConfigUtils.getString("kafka.err.log");
    private static final String kafka_start_log = ConfigUtils.getString("kafka.start.log");
    private static final String kafka_display_log = ConfigUtils.getString("kafka.display.log");
    private static final String kafka_action_log = ConfigUtils.getString("kafka.action.log");
    private static final String kafka_dirty_topic = ConfigUtils.getString("kafka.dirty.topic");
    private static final String kafka_page_topic = ConfigUtils.getString("kafka.page.topic");
    private static final OutputTag<String> errTag = new OutputTag<String>("errTag") {
    };
    private static final OutputTag<String> startTag = new OutputTag<String>("startTag") {
    };
    private static final OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
    };
    private static final OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
    };
    private static final OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
    };
    //定义了一个用于集中管理多个数据流的映射表，核心作用是将不同类型的数据流（通常是侧输出流或子数据流）通过唯一标识关联起来
    private static final HashMap<String, DataStream<String>> collectDsMap = new HashMap<>();

    @SneakyThrows
    public static void main(String[] args) {
        // 设置Hadoop用户名
        System.setProperty("HADOOP_USER_NAME", "root");
        // 打印环境变量
        CommonUtils.printCheckPropEnv(
                // false可能是一个控制开关（比如是否开启严格校验模式，false可能表示仅打印不中断程序）
                false,
                kafka_topic_base_log_data,
                kafka_bootstrap_server,
                kafka_err_log,
                kafka_start_log,
                kafka_display_log,
                kafka_action_log,
                kafka_dirty_topic,
                kafka_page_topic
        );
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //为环境配置对象设置默认参数值
        EnvironmentSettingUtils.defaultParameter(env);

        // 创建kafka数据源
        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_bootstrap_server,
                        kafka_topic_base_log_data,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ), WatermarkStrategy.noWatermarks(), "read_kafka_realtime_log"
        );

        SingleOutputStreamOperator<JSONObject> processDs = kafkaSourceDs.process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                        try {
                            collector.collect(JSONObject.parseObject(s));
                        } catch (Exception e) {
                            context.output(dirtyTag, s);
                            System.out.println("Convert JsonData Error !");
                        }
                    }
                })//对于有状态的算子（如窗口操作、状态机等），设置 uid 尤为重要；而 name 则对所有算子都推荐设置，以提高作业的可维护性。
                .uid("convert_json_process").name("convert_json_process");
        SideOutputDataStream<String> dirtyDs = processDs.getSideOutput(dirtyTag);
        dirtyDs.print("dirtyDs->");
//        processDs.print();
        dirtyDs.sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_server, kafka_dirty_topic))
                .uid("sink_dirty_data_to_kafka")
                .name("sink_dirty_data_to_kafka");
        // 通过 mid 字段对数据流进行分组
        KeyedStream<JSONObject, String> keyedStream = processDs.keyBy(obj -> obj.getJSONObject("common").getString("mid"));
//        keyedStream.print();
        SingleOutputStreamOperator<JSONObject> mapDs = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
                    // 定义一个ValueState类型的成员变量，用于存储字符串类型的"最近访问日期"
                    // ValueState是Flink的单值状态，每个key对应一个状态值
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 值状态
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>(
                                "lastVisitDateState", // 状态名称，用于在状态后端中标识该状态
                                String.class  // 状态存储的数据类型
                        );
                        // 2. 为状态设置过期时间（TTL，Time-To-Live）
                        // 当状态超过指定时间未更新时，会被自动清理，避免状态无限增长
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.seconds(10))
                                        // 状态更新策略：创建或写入时重置过期时间
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);

                    }


                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObject.getLong("ts");
                        String curVisitDate = DateTimeUtils.tsToDate(ts);
                        if ("1".equals(isNew)) {
                            //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                            if (StringsUtils.isEmpty(lastVisitDate)) {
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            //如果 is_new 的值为 0
                            //	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时
                            //日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                            if (StringsUtils.isEmpty(lastVisitDate)) {
                                String yesDay = DateTimeUtils.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesDay);
                            }
                        }
                        return jsonObject;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }
                }).uid("fix_isNew_map")
                .name("fix_isNew_map");
//        mapDs.print("mapDs->");
        SingleOutputStreamOperator<String> processTagDs = mapDs.process(new com.retailersv.func.ProcessSplitStreamFunc(errTag, startTag, displayTag, actionTag))
                .uid("flag_stream_process")
                .name("flag_stream_process");
//        processTagDs.print();
        SideOutputDataStream<String> sideOutputErrDs = processTagDs.getSideOutput(errTag);
        SideOutputDataStream<String> sideOutputStartDs = processTagDs.getSideOutput(startTag);
        SideOutputDataStream<String> sideOutputDisplayDs = processTagDs.getSideOutput(displayTag);
        SideOutputDataStream<String> sideOutputActionDs = processTagDs.getSideOutput(actionTag);

        collectDsMap.put("errTag", sideOutputErrDs);
        collectDsMap.put("startTag", sideOutputStartDs);
        collectDsMap.put("displayTag", sideOutputDisplayDs);
        collectDsMap.put("actionTag", sideOutputActionDs);
        collectDsMap.put("page", processTagDs);

        SplitDs2kafkaTopicMsg(collectDsMap);

//        env.disableOperatorChaining() 是 Apache Flink 中用于禁用算子链（Operator Chaining） 的操作。

        env.disableOperatorChaining();
        env.execute();
    }

    public static void SplitDs2kafkaTopicMsg(HashMap<String, DataStream<String>> dataStreamHashMap) {
        dataStreamHashMap.get("errTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_server, kafka_err_log))
                .uid("sk_errMsg2Kafka")
                .name("_sk_errMsg2Kafka");
        dataStreamHashMap.get("startTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_server, kafka_start_log))
                .uid("sk_startMsg2Kafka")
                .name("_sk_startMsg2Kafka");
        dataStreamHashMap.get("displayTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_server, kafka_display_log))
                .uid("sk_displayMsg2Kafka")
                .name("_sk_displayMsg2Kafka");
        dataStreamHashMap.get("actionTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_server, kafka_action_log))
                .uid("sk_actionMsg2Kafka")
                .name("_sk_actionMsg2Kafka");
        dataStreamHashMap.get("page").sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_server, kafka_page_topic))
                .uid("sk_pageMsg2Kafka")
                .name("_sk_pageMsg2Kafka");
        dataStreamHashMap.get("errTag").print("errTag ->");
        dataStreamHashMap.get("startTag").print("startTag ->");
        dataStreamHashMap.get("displayTag").print("displayTag ->");
        dataStreamHashMap.get("actionTag").print("actionTag ->");
        dataStreamHashMap.get("page").print("page ->");
    }
}
