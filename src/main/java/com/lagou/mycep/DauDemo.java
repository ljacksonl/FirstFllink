package com.lagou.mycep;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class DauDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<UserBean> data = env.fromElements(
                new UserBean("100XX", 0.0D, 1597905234000L),
                new UserBean("100XX", 100.0D, 1597905235000L),
                new UserBean("100XX", 200.0D, 1597905236000L),
                new UserBean("100XX", 300.0D, 1597905237000L),
                new UserBean("100XX", 400.0D, 1597905238000L),
                new UserBean("100XX", 500.0D, 1597905239000L),
                new UserBean("101XX", 0.0D, 1597905240000L),
                new UserBean("101XX", 100.0D, 1597905241000L)
        );

        SingleOutputStreamOperator<UserBean> watermarks = data.assignTimestampsAndWatermarks(new WatermarkStrategy<UserBean>() {
            @Override
            public WatermarkGenerator<UserBean> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<UserBean>() {
                    long maxTimeStamp = Long.MIN_VALUE;
                    long maxOutOfOrderness = 500l;

                    @Override
                    public void onEvent(UserBean event, long eventTimestamp, WatermarkOutput output) {
                        maxTimeStamp = Math.max(maxTimeStamp, event.getTs());
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimeStamp - maxOutOfOrderness));
                    }
                };
            }
        }.withTimestampAssigner((element, recordTimestamp) -> element.getTs()));
        KeyedStream<UserBean, String> keyed = watermarks.keyBy(value -> value.getUid());

        //做出pattern
        Pattern<UserBean, UserBean> pattern = Pattern.<UserBean>begin("begin").where(new IterativeCondition<UserBean>() {
            @Override
            public boolean filter(UserBean value, Context<UserBean> ctx) throws Exception {
                return value.getMoney() > 0;
            }
        })
                .timesOrMore(5)
                .within(Time.hours(24));

        PatternStream<UserBean> patternStream = CEP.pattern(keyed, pattern);

        SingleOutputStreamOperator<String> result = patternStream.process(new PatternProcessFunction<UserBean, String>() {
            @Override
            public void processMatch(Map<String, List<UserBean>> match, Context ctx, Collector<String> out) throws Exception {
                out.collect(match.get("begin").get(0).getUid());
            }
        });

        result.print();
        env.execute();

    }
}
