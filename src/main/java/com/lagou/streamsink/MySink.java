package com.lagou.streamsink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MySink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.fromElements("hello");
        data.addSink(new SinkFunction<String>() {
            @Override
            //唤醒,insert into 写到哪里去.
            public void invoke(String value, Context context) throws Exception {

            }
        });
    }
}
