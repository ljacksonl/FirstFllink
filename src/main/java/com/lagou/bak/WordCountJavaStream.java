package com.lagou.bak;

import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.omg.CosNaming._BindingIteratorImplBase;

import java.util.HashMap;

public class WordCountJavaStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = executionEnvironment.socketTextStream("linux121", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : s.split(" ")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).keyBy(0).sum(1);
        sum.print();
        executionEnvironment.execute();

    }
}
