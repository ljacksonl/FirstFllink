package com.lagou.streamdatasource;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class SourceFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> data = env.fromElements("spark", "flink");
        ArrayList<People> peopleList = new ArrayList<People>();
        peopleList.add(new People("lucas",18));
        peopleList.add(new People("jack",28));
        peopleList.add(new People("lucy",38));


        DataStreamSource<People> data = env.fromCollection(peopleList);
        SingleOutputStreamOperator<People> filtered = data.filter(new FilterFunction<People>() {
            @Override
            public boolean filter(People people) throws Exception {

                return people.age > 20;
            }
        });
        filtered.print();


            /*SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndeOne = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    for (String word : s.split(",")) {
                        collector.collect(new Tuple2<String, Integer>(word, 1));
                    }
                }
        });

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = wordAndeOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = tuple2StringKeyedStream.sum(1);*/
//        result.print();
        env.execute();
    }

    static class People {
        String name;
        Integer age;

        public People(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "People{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
