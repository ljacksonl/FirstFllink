package com.lagou.streamdatasource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceFromNoParallelSourceRun {
    //机器是8核，模拟8个slot
    //一个事情一个slot来做
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.addSource(new SourceFromNoParallelSource());
        data.print();
        env.execute();
    }
}
