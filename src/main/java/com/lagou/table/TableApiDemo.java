package com.lagou.table;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import static org.apache.flink.table.api.Expressions.$;

public class TableApiDemo {
    public static void main(String[] args) throws Exception {
        //Flink执行环境env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //用env，做出Table环境tEnv
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //table环境的重载方法：
        /*EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
//                .inBatchMode()
                .inStreamingMode()
                .build();
        StreamTableEnvironment.create(env,settings);*/

        //获取流式数据源
       DataStreamSource<Tuple2<String, Integer>> data = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                int num = 0;
                while (true) {
                    num++;
                    ctx.collect(new Tuple2<>("name"+num, num));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

       //从kafka上获取数据
        /*ConnectTableDescriptor descriptor = tEnv.connect(
                // declare the external system to connect to
                new Kafka()
                        .version("universal")
                        .topic("animal")
                        .startFromEarliest()
                        .property("bootstrap.servers", "hdp-2:9092")
        )

                // declare a format for this system
                .withFormat(
//                        new Json()
                        new Csv()
                )

                // declare the schema of the table
                .withSchema(
                        new Schema()
//                                .field("rowtime", DataTypes.TIMESTAMP(3))
//                                .rowtime(new Rowtime()
//                                        .timestampsFromField("timestamp")
//                                        .watermarksPeriodicBounded(60000)
//                                )
//                                .field("user", DataTypes.BIGINT())
                                .field("message", DataTypes.STRING())
                );
        // create a table with given name
        descriptor.createTemporaryTable("MyUserTable");

        Table table1 = tEnv.sqlQuery("select * from MyUserTable");
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(table1, Row.class);
        tuple2DataStream.print();*/


//        tEnv.connect(new FileSystem().path("d:\\data\\input"))// 定义表数据来源，外部连接
//                .withFormat(new Csv()) // 定义从外部系统读取数据之后的格式化方法
//                .withSchema(new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("name",DataTypes.STRING())
////                        .field("timestamp", DataTypes.BIGINT())
////                        .field("temperature", DataTypes.DOUBLE())
//                ) // 定义表结构
//                .createTemporaryTable("inputTable"); // 创建临时表
//        Table resultTable = tEnv.sqlQuery("select * from inputTable");
//        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(resultTable, Row.class);
//        result.print();

//        PipelineExecutor

        //Table方式
        //将流式数据源做成Table
        Table table = tEnv.fromDataStream(data, $("name"), $("age"));
        //对Table中的数据做查询
        Table name = table.select($("name"));
        Table filtered = table.select($("name"), $("age")).filter($("age").mod(2).isEqual(0));

        //将处理结果输出到控制台
        //Boolean,true是一个add增加的数据，false是可撤回的数据
        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(name, Row.class);

//        Table mingzi = table.select($("name").as("mingzi"));
//        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(mingzi, Row.class);

//        GroupedTable groupedTable = table.select($("name"), $("age")).groupBy($("age").mod(2).as("jiou"));
//        tEnv.toRetractStream(groupedTable,Row.class);

        //SQL方式：
        /*tEnv.createTemporaryView("userss",data, $("name"), $("age"));
        String s = "select name,age from userss where mod(age,2)=0";
        Table table = tEnv.sqlQuery(s);
        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(table, Row.class);*/

       /* tEnv.connect(new FileSystem().path("D:\\data\\out.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema().field("name", DataTypes.STRING()).field("age",DataTypes.INT()))
                .createTemporaryTable("outputTable");
        filtered.executeInsert("outputTable");*/


       //往kafka上输出表
        /*DataStreamSource<String> data = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int num = 0;
                while (true) {
                    num++;
                    ctx.collect("name"+num);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        Table name = tEnv.fromDataStream(data, $("name"));

        ConnectTableDescriptor descriptor = tEnv.connect(
                // declare the external system to connect to
                new Kafka()
                        .version("universal")
                        .topic("animal")
                        .startFromEarliest()
                        .property("bootstrap.servers", "hdp-2:9092")
        )

                // declare a format for this system
                .withFormat(
//                        new Json()
                        new Csv()
                )

                // declare the schema of the table
                .withSchema(
                        new Schema()
//                                .field("rowtime", DataTypes.TIMESTAMP(3))
//                                .rowtime(new Rowtime()
//                                        .timestampsFromField("timestamp")
//                                        .watermarksPeriodicBounded(60000)
//                                )
//                                .field("user", DataTypes.BIGINT())
                                .field("message", DataTypes.STRING())
                );
        // create a table with given name
        descriptor.createTemporaryTable("MyUserTable");

        name.executeInsert("MyUserTable");*/

        //往mysql输出表数据
//        DataStreamSource<String> data = env.addSource(new SourceFunction<String>() {
//            @Override
//            public void run(SourceContext<String> ctx) throws Exception {
//                int num = 0;
//                while (true) {
//                    num++;
//                    ctx.collect("name"+num);
//                    Thread.sleep(1000);
//                }
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        });
//
//        Table name = tEnv.fromDataStream(data, $("name"));
//
//
//
//        String sql = "create table jdbcOutputTable (name varchar(20) not null) with ('connector.type' = 'jdbc','connector.url' = 'jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC','connector.table' = 'name','connector.driver' = 'com.mysql.cj.jdbc.Driver','connector.username' = 'root','connector.password' = 'lucas')";
//
//        tEnv.sqlUpdate(sql);
////        TableResult tableResult = tEnv.executeSql(sql);
////        tableResult.
//        name.executeInsert("jdbcOutputTable");


        result.print();
        env.execute();

    }
}
