package com.retailersv.dws;

import com.alibaba.fastjson.JSONObject;
import com.retailersv.domain.TrafficHomeDetailPageViewBean;
import com.retailersv.func.BeanToJsonStrMapFunction;
import com.stream.common.utils.*;
import com.stream.utils.DorisUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;


public class DbusDwsTrafficHomeDetailPageView2Doris {
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_page_topic = ConfigUtils.getString("kafka.page.topic");

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(1);

        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_bootstrap_server,
                        kafka_page_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ), WatermarkStrategy.noWatermarks(), "read_kafka_page_topic"
        );
        SingleOutputStreamOperator<JSONObject> filterDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        return "home".equals(pageId) || "good_detail".equals(pageId);
                    }
                });
//        filterDs.print();

        SingleOutputStreamOperator<JSONObject> watermarkDs = filterDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        })
        );
        KeyedStream<JSONObject, String> keyedDs = watermarkDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processDs = keyedDs.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeLastVisitTimeState;
            private ValueState<String> detailLastVisitTimeState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeValueStateDescriptor = new ValueStateDescriptor<>("homeLastVisitTimeState", String.class);
                homeValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                homeLastVisitTimeState = getRuntimeContext().getState(homeValueStateDescriptor);
                ValueStateDescriptor<String> detailValueStateDescriptor = new ValueStateDescriptor<>("detailLastVisitTimeState", String.class);
                detailValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                detailLastVisitTimeState = getRuntimeContext().getState(detailValueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                Long ts = jsonObj.getLong("ts");
                String curVisitDate = DateTimeUtils.tsToDate(ts);
                Long homeUvCt = 0L;
                Long detailUvCt = 0L;
                if ("home".equals(pageId)) {
                    String homeLastVisitDate = homeLastVisitTimeState.value();
                    if (StringsUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                        homeUvCt = 1L;
                        homeLastVisitTimeState.update(curVisitDate);
                    }
                } else {
                    String detailLastVisitDate = detailLastVisitTimeState.value();
                    if (StringsUtils.isEmpty(detailLastVisitDate) || !detailLastVisitDate.equals(curVisitDate)) {
                        detailUvCt = 1L;
                        detailLastVisitTimeState.update(curVisitDate);
                    }
                }

                if (homeUvCt != 0L || detailUvCt != 0L) {
                    collector.collect(
                            new TrafficHomeDetailPageViewBean(
                                    "", "", "",
                                    homeUvCt, detailUvCt, ts
                            )
                    );
                }
            }
        });
//        processDs.print();

        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDs = processDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)));

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDs = windowDs.reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {

            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean t1, TrafficHomeDetailPageViewBean t2) throws Exception {
                t1.setHomeUvCt(t1.getHomeUvCt() + t2.getHomeUvCt());
                t1.setGoodDetailUvCt(t1.getGoodDetailUvCt() + t2.getGoodDetailUvCt());
                return t1;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                TrafficHomeDetailPageViewBean bean = iterable.iterator().next();
                String stt = DateTimeUtils.tsToDateTime(timeWindow.getStart());
                String edt = DateTimeUtils.tsToDateTime(timeWindow.getEnd());
                String curDate = DateTimeUtils.tsToDate(timeWindow.getStart());
                bean.setStt(stt);
                bean.setEdt(edt);
                bean.setCurDate(curDate);
                collector.collect(bean);
            }
        });
        reduceDs.print();
        reduceDs.map(new BeanToJsonStrMapFunction<TrafficHomeDetailPageViewBean>())
                .sinkTo(DorisUtils.getDorisSink("dws_traffic_home_detail_page_view_window"));


        env.execute();
    }
}
