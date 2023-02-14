package com.lagou.streamsink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MySinkToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("linux123",7777);

        SingleOutputStreamOperator<Tuple2<String, String>> m_word = data.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<String, String>("m_word", value);
            }
        });

        FlinkJedisPoolConfig.Builder builder = new FlinkJedisPoolConfig.Builder();
        builder.setHost("linux123");
        builder.setPort(6379);

        FlinkJedisPoolConfig config = builder.build();

        //
        RedisSink redisSink = new RedisSink(config, new RedisMapper<Tuple2<String, String>>() {

            //获取命令的描述符信息
            @Override
            public RedisCommandDescription getCommandDescription() {
                //以List的方式push
                return new RedisCommandDescription(RedisCommand.LPUSH);
            }

            //获取key保存在redis
            @Override
            public String getKeyFromData(Tuple2<String, String> data) {
                return data.f0;
            }

            //获取value保存在redis
            @Override
            public String getValueFromData(Tuple2<String, String> data) {
                return data.f1;
            }
        });
        m_word.addSink(redisSink);
        env.execute();
    }
}
