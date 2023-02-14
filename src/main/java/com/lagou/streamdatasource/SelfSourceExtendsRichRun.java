package com.lagou.streamdatasource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SelfSourceExtendsRichRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.addSource(new SelfSourceParallelExtendsRich());
        data.print();
        env.execute();
    }
}


