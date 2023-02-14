package com.lagou.streamtransformation;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

//Split the stream into two or more streams according to some criterion.
//Select one or more streams from a split stream.
public class SplitSelectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> data = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
        SplitStream<Integer> splited = data.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                ArrayList<String> output = new ArrayList<>();
                if (value % 2 == 0) {
                    //偶数流
                    output.add("even");
                } else {
                    //奇数流
                    output.add("odd");
                }
                return output;
            }
        });

        DataStream<Integer> event = splited.select("even");
        event.print();
        env.execute();
    }
}
