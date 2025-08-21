package com.retailersv.dws;

import com.alibaba.fastjson.JSONObject;
import com.retailersv.domain.TrafficPageViewBean;
import com.retailersv.func.BeanToJsonStrMapFunction;
import com.stream.common.utils.*;
import com.stream.utils.DorisUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;


public class DbusDwsTrafficVcChArIsNewPageView2Doris {
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_page_topic = ConfigUtils.getString("kafka.page.topic");


    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_bootstrap_server,
                        kafka_page_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ), WatermarkStrategy.noWatermarks(), "read_kafka_page_topic"
        );


        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSourceDs.map(JSONObject::parseObject);
        KeyedStream<JSONObject, String> midKeyDs = jsonObjDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<TrafficPageViewBean> beanDs = midKeyDs.map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {
            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                JSONObject common = jsonObj.getJSONObject("common");
                JSONObject page = jsonObj.getJSONObject("page");
                // 从状态中获取上次访问的日期
                Long ts = jsonObj.getLong("ts");
                String curVisitDate = DateTimeUtils.tsToDate(ts);
                String lastVisitDate = lastVisitDateState.value();
                Long uvCt = 0L;
                if (StringsUtils.isEmpty(lastVisitDate) ||
                        !lastVisitDate.equals(curVisitDate)) {
                    uvCt = 1L;
                    lastVisitDateState.update(curVisitDate);
                }

                String lastPageId = page.getString("last_page_id");
                Long svCt = StringsUtils.isEmpty(lastPageId) ? 1L : 0L;
                return new TrafficPageViewBean(
                        "", "", "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        uvCt,
                        svCt,
                        1L,
                        page.getLong("during_time"),
                        ts
                );
            }
        });

//        beanDs.print();
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean bean, long l) {
                                return bean.getTs();
                            }
                        })
        );

        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyDs = withWatermarkDs.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                        return Tuple4.of(bean.getVc(), bean.getCh(), bean.getAr(), bean.getIsNew());
                    }
                }
        );
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDs =
                dimKeyDs.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        SingleOutputStreamOperator<TrafficPageViewBean> reduceDs = windowDs.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean t1, TrafficPageViewBean t2) throws Exception {
                t1.setPvCt(t1.getPvCt() + t2.getPvCt());
                t1.setUvCt(t1.getUvCt() + t2.getUvCt());
                t1.setSvCt(t1.getSvCt() + t2.getSvCt());
                t1.setDurSum(t1.getDurSum() + t2.getDurSum());
                return t1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4,
                              TimeWindow timeWindow,
                              Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                TrafficPageViewBean pageViewBean = iterable.iterator().next();
                String stt = DateTimeUtils.tsToDateTime(timeWindow.getStart());
                String edt = DateTimeUtils.tsToDateTime(timeWindow.getEnd());
                String curDate = DateTimeUtils.tsToDate(timeWindow.getStart());
                pageViewBean.setStt(stt);
                pageViewBean.setEdt(edt);
                pageViewBean.setCur_date(curDate);
                collector.collect(pageViewBean);
            }
        });
        reduceDs.print();
        reduceDs.map(new BeanToJsonStrMapFunction<TrafficPageViewBean>())
                .sinkTo(DorisUtils.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));


        env.execute();
    }
}
