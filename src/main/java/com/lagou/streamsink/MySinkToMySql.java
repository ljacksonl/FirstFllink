package com.lagou.streamsink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySinkToMySql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String url = "jdbc:mysql://192.168.80.102:3306/bigdata?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC";
        String user = "hive";
        String password = "12345678";
        Student stu1 = new Student("lucas", 18);
        Student stu2 = new Student("jack", 28);
        DataStreamSource<Student> data = env.fromElements(stu1,stu2);

        data.addSink(new RichSinkFunction<Student>() {
            Connection connection = null;
            PreparedStatement preparedStatement = null;
            //open初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection(url, user, password);
                String sql = "insert into student (name,age) values (?,?)";
                preparedStatement = connection.prepareStatement(sql);
            }

            //唤醒,insert into 写到哪里去.所有可执行指令
            @Override
            public void invoke(Student value, Context context) throws Exception {
                preparedStatement.setString(1,value.getName());
                preparedStatement.setInt(2,value.getAge());
                preparedStatement.executeUpdate();
            }

            @Override
            public void close() throws Exception {
                if(connection != null) {
                    connection.close();
                }
                if(preparedStatement != null) {
                    preparedStatement.close();
                }
            }
        });
        env.execute();
    }
}
