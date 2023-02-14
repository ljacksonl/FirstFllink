package com.lagou.bak;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * 1、读取数据源
 * 2、处理数据源
 * a、将读到的数据源文件中的每一行根据空格切分
 * b、将切分好的每个单词拼接1
 * c、根据单词聚合（将相同的单词放在一起）
 * d、累加相同的单词（单词后面的1进行累加）
 * 3、保存处理结果
 */

public class WordCountJavaBatch {
    public static void main(String[] args) throws Exception {
        String inputPath = "D:\\core\\FirstFilnk\\data\\input\\hello.txt";
        String outputPath = "D:\\core\\FirstFilnk\\data\\output";

        //获取flink的运行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = executionEnvironment.readTextFile(inputPath);
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOnes = text.flatMap(new SplitClz());

        //(hello 1)(you 1)
        UnsortedGrouping<Tuple2<String, Integer>> groupWordAndOne = wordAndOnes.groupBy(0);
        //(hello,1)(hello,1)
        AggregateOperator<Tuple2<String, Integer>> out = groupWordAndOne.sum(1);

        //setParallelism并行度 1
        out.writeAsCsv(outputPath,"\n"," ").setParallelism(1);

        //调用方法才能
        executionEnvironment.execute();

    }

    //泛型的意思是：String，传进去的数据源，Integer单词统计的1。
    static class SplitClz implements FlatMapFunction<String ,Tuple2<String,Integer>>{

        //collector:向下游数据发送,以便拼接。
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] s1 = s.split(" ");
            for (String word : s1) {
                //collect发送到下游去
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
