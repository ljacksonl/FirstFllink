package com.lagou.time;

import net.minidev.json.JSONUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class WatermarkDemo {
    /**
     * 1、获取数据源
     *
     * 2、转化
     *
     * 3、声明水印（watermark）
     *
     * 4、分组聚合，调用window的操作
     *
     * 5、保存处理结果
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //处理的时间是一个eventtime。
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //一秒钟生成一个watermark
        env.getConfig().setAutoWatermarkInterval(1000L);
        //并行度
        env.setParallelism(1);
        DataStreamSource<String> data = env.socketTextStream("linux121", 7777);
        SingleOutputStreamOperator<Tuple2<String, Long>> maped = data.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<String, Long>(split[0], Long.valueOf(split[1]));
            }
        });
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        //加水印
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarks = maped.assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {
            @Override
            //创建水印的生成器
            //策略
            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Tuple2<String, Long>>() {
                    private long maxTimeStamp = 0L;

                    @Override
                    //来一个数据处理一次
                    //可以往外发送watermark
                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                        //拿出最大时间戳，生成事件时间
                        //水印时间= 事件时间-最大延迟时间
                        //event.f1 第二个元素是时间戳
                        maxTimeStamp = Math.max(maxTimeStamp, event.f1);
                        System.out.println("maxTimeStamp:" + maxTimeStamp + "...format:" + sdf.format(maxTimeStamp));
                    }

                    @Override
                    //周期性去放出watermark
                    //周期性每段事件做一个watermark，一个事件来一个watermark
                    //每一秒钟输出一次
                    public void onPeriodicEmit(WatermarkOutput output) {
                        System.out.println(".....onPeriodicEmit....");
                        long maxOutOfOrderness = 3000l;
                        //传播水印是广播方式
                        output.emitWatermark(new Watermark(maxTimeStamp - maxOutOfOrderness));
                    }
                };
            }
            //根据哪一个字段生成watermark
        }.withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                //第二个元素
                return element.f1;
            }
        }));

        //
        KeyedStream<Tuple2<String, Long>, String> keyed = watermarks.keyBy(value -> value.f0);
        //滚动窗口
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> windowed = keyed.window(TumblingEventTimeWindows.of(Time.seconds(4)));
        SingleOutputStreamOperator<String> result = windowed.apply(new WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {

                System.out.println("..." + sdf.format(window.getStart()));
                String key = s;
                Iterator<Tuple2<String, Long>> iterator = input.iterator();
                ArrayList<Long> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    Tuple2<String, Long> next = iterator.next();
                    list.add(next.f1);
                }
                //java的排序方法
                Collections.sort(list);
                String result = "key:" + key + "..." + "list.size:" + list.size() + "...list.first:" + sdf.format(list.get(0)) + "...list.last:" + sdf.format(list.get(list.size() - 1)) + "...window.start:" + sdf.format(window.getStart()) + "..window.end:" + sdf.format(window.getEnd());
                out.collect(result);
            }
        });

        result.print();
        env.execute();
    }

}


















