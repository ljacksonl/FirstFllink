package com.lagou.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

public class WindowDemoCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("linux121", 7777);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        //2、获取窗口
        SingleOutputStreamOperator<Tuple3<String, String, String>> maped = data.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {

                long l = System.currentTimeMillis();
                String dataTime = sdf.format(l);
                Random random = new Random();
                int randomNum = random.nextInt(5);
                return new Tuple3<>(value, dataTime, String.valueOf(randomNum));
            }
        });
        KeyedStream<Tuple3<String, String, String>, String> keybyed = maped.keyBy(value -> value.f0);

        //countWindow()来3个数据源就进行输出，接收3个数据为一个窗口(不分key)
        WindowedStream<Tuple3<String, String, String>, String, GlobalWindow> countWindow = keybyed.countWindow(3,1);

        SingleOutputStreamOperator<String> applyed = countWindow.apply(new WindowFunction<Tuple3<String, String, String>, String, String, GlobalWindow>() {
            @Override
            //s是数据源本身，也就是key
            public void apply(String s, GlobalWindow window, Iterable<Tuple3<String, String, String>> input, Collector<String> out) throws Exception {
                Iterator<Tuple3<String, String, String>> iterator = input.iterator();
                StringBuilder sb = new StringBuilder();
                while (iterator.hasNext()) {
                    Tuple3<String, String, String> next = iterator.next();
                    sb.append(next.f0 + ".." + next.f1 + ".." + next.f2);
                }
//                window.
                out.collect(sb.toString());
            }
        });

        applyed.print();
        env.execute();


    }
}
