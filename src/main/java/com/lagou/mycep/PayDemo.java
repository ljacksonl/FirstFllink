package com.lagou.mycep;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class PayDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<PayBean> data = env.fromElements(
                new PayBean(1L, "create", 1597905234000L),
                new PayBean(1L, "pay", 1597905235000L),
                new PayBean(2L, "create", 1597905236000L),
                new PayBean(2L, "pay", 1597905237000L),
                new PayBean(3L, "create", 1597905239000L)
        );
        SingleOutputStreamOperator<PayBean> watermarks = data.assignTimestampsAndWatermarks(new WatermarkStrategy<PayBean>() {
            @Override
            public WatermarkGenerator<PayBean> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<PayBean>() {
                    long maxTimeStamp = Long.MIN_VALUE;
                    long maxOutOfOrderness = 500l;

                    @Override
                    public void onEvent(PayBean event, long eventTimestamp, WatermarkOutput output) {
                        maxTimeStamp = Math.max(maxTimeStamp, event.getTs());
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimeStamp - maxOutOfOrderness));
                    }
                };
            }
        }.withTimestampAssigner((element, recordTimestamp) -> element.getTs()));
        KeyedStream<PayBean, Long> keyed = watermarks.keyBy(value -> value.getId());

        //pattern
        Pattern<PayBean, PayBean> pattern = Pattern.<PayBean>begin("start").where(new IterativeCondition<PayBean>() {
            @Override
            public boolean filter(PayBean value, Context<PayBean> ctx) throws Exception {
                return value.getState().equals("create");
            }
        })
                .followedBy("next").where(new IterativeCondition<PayBean>() {
                    @Override
                    public boolean filter(PayBean value, Context<PayBean> ctx) throws Exception {
                        return value.getState().equals("pay");
                    }
                })
                .within(Time.seconds(600));

        PatternStream<PayBean> patternStream = CEP.pattern(keyed, pattern);

        //select
        OutputTag<PayBean> outoftime = new OutputTag<PayBean>("outoftime"){};
        SingleOutputStreamOperator<PayBean> result = patternStream.select(outoftime, new PatternTimeoutFunction<PayBean, PayBean>() {
            @Override
            public PayBean timeout(Map<String, List<PayBean>> pattern, long timeoutTimestamp) throws Exception {
                return pattern.get("start").get(0);
            }
        }, new PatternSelectFunction<PayBean, PayBean>() {
            @Override
            public PayBean select(Map<String, List<PayBean>> pattern) throws Exception {
                return pattern.get("start").get(0);
            }
        });

        DataStream<PayBean> sideOutput = result.getSideOutput(outoftime);
        sideOutput.print();

        env.execute();

    }
}
