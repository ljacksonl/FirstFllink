package com.lagou.streamdatasource;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

public class SourceFromKafka {
    public static void main(String[] args) throws Exception {
        String topic = "tp_demo_01";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","linux121:9092");


        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.addSource(consumer);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndeOne = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            //s是输入，collector是输出
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //hello,you,me,hello  [hello  you   me]
                for (String word : s.split(",")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1)); // (hello 1) (hello 1)
                }
            }
        });

        //传进来是一个Tuple2，key的类型是String。
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = wordAndeOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //根据第二个二元组累加，
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = tuple2StringKeyedStream.sum(1);
        result.print();
        env.execute();
    }
}
