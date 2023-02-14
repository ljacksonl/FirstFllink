package com.lagou.streamdatasource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class SelfSourceParallelExtendsRich extends RichParallelSourceFunction<String> {
    long count;
    boolean isRunning;

    @Override
    //Open方法起到一个实例化的作用
    public void open(Configuration parameters) throws Exception {
        System.out.println("....open");
        count = 0;
        isRunning = true;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning) {
            ctx.collect(String.valueOf(count) + "...from Rich");
            count ++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
