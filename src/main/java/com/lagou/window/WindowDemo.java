package com.lagou.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

public class WindowDemo {

    //

    //

    //



    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1、获取流数据源
        DataStreamSource<String> data = env.socketTextStream("linux121", 7777);
        DataStreamSource<String> data1 = env.addSource(new SourceFunction<String>() {
            int count = 0;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    ctx.collect(count + "号数据源");
                    count++;
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        //2、获取窗口
        SingleOutputStreamOperator<Tuple3<String, String, String>> maped = data1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {

                long l = System.currentTimeMillis();
                String dataTime = sdf.format(l);
                Random random = new Random();
                int randomNum = random.nextInt(5);
                return new Tuple3<>(value, dataTime, String.valueOf(randomNum));
            }
        });
        //把相同的key放在一起，并发执行。
        KeyedStream<Tuple3<String, String, String>, String> keybyed = maped.keyBy(value -> value.f0);

        //Time.seconds(2)加上这个，就成为了滑动窗口了。
        WindowedStream<Tuple3<String, String, String>, String, TimeWindow> timeWindow = keybyed.timeWindow(Time.seconds(5),Time.seconds(2));



       //3、操作窗口数据apply方法
        //Tuple3输入，String输出，String(key的数据类型),TimeWindow(处理的是时间类型窗口)
        SingleOutputStreamOperator<String> applyed = timeWindow.apply(new WindowFunction<Tuple3<String, String, String>, String, String, TimeWindow>() {
            @Override
            //s是数据源本身，window当前窗口，Tuple3(有可能是相同的数据，就放到迭代器中),out输出。
            public void apply(String s, TimeWindow window, Iterable<Tuple3<String, String, String>> input, Collector<String> out) throws Exception {
                Iterator<Tuple3<String, String, String>> iterator = input.iterator();
                StringBuilder sb = new StringBuilder();
                System.out.println("...............");
                while (iterator.hasNext()) {
                    Tuple3<String, String, String> next = iterator.next();
                    sb.append(next.f0 + "..." + next.f1 + "..." + next.f2);

                }
                //window.getStart()获取窗口的开始时间，window.getEnd()获取窗口的结束时间
                String s1 = s + "..." + sdf.format(window.getStart()) + "..." + sdf.format(window.getEnd())+ "..." + sb;
                out.collect(s1);

            }
        });
        //4、输出窗口数据
        applyed.print();
        env.execute();


    }
}

















