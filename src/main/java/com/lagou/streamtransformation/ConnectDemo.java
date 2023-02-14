package com.lagou.streamtransformation;

import com.lagou.streamdatasource.SelfSourceParallel;
import com.lagou.streamdatasource.SelfSourceParallelExtendsRich;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        //拿到上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建两个数据源
        DataStreamSource<String> data1 = env.addSource(new SelfSourceParallel());
        DataStreamSource<String> data2 = env.addSource(new SelfSourceParallelExtendsRich());
        //connect连接数据源
        ConnectedStreams<String, String> connected = data1.connect(data2);
        //CoMap 作用在两个数据源上
        //CoMapFunction<String, String, String>两个输入是String，输出是String
        SingleOutputStreamOperator<String> maped = connected.map(new CoMapFunction<String, String, String>() {
           //处理第一个数据源
            @Override
            public String map1(String value) throws Exception {
                return value;
            }
            //处理第二个数据源
            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });

        maped.print();
        env.execute();

    }
}
