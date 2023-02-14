package com.lagou.streamdatasource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SelfSourceParallelRun {
    public static void main(String[] args) throws Exception {
        //8个并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.addSource(new SelfSourceParallel());
        data.print();
        env.execute();
    }
}
