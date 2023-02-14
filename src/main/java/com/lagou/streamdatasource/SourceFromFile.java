package com.lagou.streamdatasource;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SourceFromFile {
    public static void main(String[] args) throws Exception {
        String inputPath = "D:\\core\\FirstFilnk\\data\\input\\hello.txt";
        String inputHdfsPath = "hdfs://linux121:9000//wcinput";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从文件获取数据源
        DataStreamSource<String> data = env.readTextFile(inputHdfsPath);

//        DataStreamSource<String> data = env.socketTextStream("hdp-1", 7777);

        /*data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {

            }
        })*/

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndeOne = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            //s是输入，collector是输出
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //hello,you,me,hello  [hello  you   me]
                for (String word : s.split(" ")) {
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
