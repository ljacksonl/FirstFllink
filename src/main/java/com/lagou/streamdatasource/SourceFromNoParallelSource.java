package com.lagou.streamdatasource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 没有并行度的自定义数据源
 */
//String 进来的数据是String类型
public class SourceFromNoParallelSource implements SourceFunction<String> {
    long count = 0;
    boolean isRunning = true;

    //将我们生成的数据源发送到下游
    //run也可以产生数据。
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        //每隔一秒生成一个数据
        while (isRunning) {
            //collect方法，就是往下游发送数据,泛型是String类型，需要类型转换
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
