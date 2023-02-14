package com.lagou.streamdatasource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class SelfSourceParallel implements ParallelSourceFunction<String> {
    long count = 0;
    boolean isRunning = true;
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning) {
            ctx.collect(String.valueOf(count));
            count ++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
