package flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * @Author: lsl
 * @Date: 2020/11/30 11:31
 * @Description:
 **/

public class SplitAndUnionStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> stringDataStreamSource = env.readTextFile("D:\\easylife\\FlinkTest\\real_time_broker\\src\\main\\resources\\text1.txt");

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost",8888,'\n');
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> map = stringDataStreamSource.map(new MapFunction<String, Tuple3<String, Integer, String>>() {

            @Override
            public Tuple3<String, Integer, String> map(String s) throws Exception {

                System.out.println(s);
                String[] split = s.split(",");
                return new Tuple3<>(split[0], Integer.valueOf(split[1]), split[2]);
            }
        });

        //拆流
        SplitStream<Tuple3<String, Integer, String>> split = map.split(new OutputSelector<Tuple3<String, Integer, String>>() {
            @Override
            public Iterable<String> select(Tuple3<String, Integer, String> value) {
                return value.f1 > 50 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<Tuple3<String, Integer, String>> high = split.select("high");
        DataStream<Tuple3<String, Integer, String>> low = split.select("low");

        //合流
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> all = high.connect(low).map(new CoMapFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map1(Tuple3<String, Integer, String> value) throws Exception {
                return value;
            }

            @Override
            public Tuple3<String, Integer, String> map2(Tuple3<String, Integer, String> value) throws Exception {
                return value;
            }
        });


        SingleOutputStreamOperator<String> flatall = low.connect(high).flatMap(new CoFlatMapFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>, String>() {
            @Override
            public void flatMap1(Tuple3<String, Integer, String> value, Collector<String> out) throws Exception {

                String s = value.f0 + value.f1 + value.f2;
                out.collect(s);

            }

            @Override
            public void flatMap2(Tuple3<String, Integer, String> value, Collector<String> out) throws Exception {
                String s = value.f0 + value.f1 + value.f2;
                out.collect(s);

            }
        });


        DataStream<Tuple3<String, Integer, String>> union = high.union(low, all);

        //connect只能合并俩个流,类型可以不同
        //union 可以合并多个类型相同的流

//        flatall.print();


        SingleOutputStreamOperator<Tuple3<String, Integer, String>> reduce = map.keyBy(data -> data.f0).reduce(new ReduceFunction<Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> reduce(Tuple3<String, Integer, String> old, Tuple3<String, Integer, String> news) throws Exception {
                int max = Math.max(old.f1, news.f1);
                int i = old.f2.compareTo(news.f2);
                String time = news.f2;
                if (i == -1) {
                    time = news.f2;
                }
                if (i == 1) {
                    time = old.f2;
                }
                return new Tuple3(news.f0, max, time);
            }
        });


        DataStreamSink<Object> process = reduce.process(new ProcessFunction<Tuple3<String, Integer, String>, Object>() {
            @Override
            public void processElement(Tuple3<String, Integer, String> value, Context ctx, Collector<Object> out) throws Exception {

                out.collect(value.f1+"--"+value.f2);

            }
        }).print();


//        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();


//        reduce.addSink(new RedisSink<>(config,new MyRedisSink("test")));



        //maxby取到第二列最大值外,其它列的数据以最大的一行为准为准
        //max取到第二列最大值外,其它列的数据以第一条为准
//                .max(1)
//                .print().setParallelism(1);
        env.execute();
    }


     static class MyRedisSink implements RedisMapper<Tuple3<String, Integer, String>> {

        String key;

         public MyRedisSink(String name) {
             key=name;
         }

         @Override
         public RedisCommandDescription getCommandDescription() {
             return new RedisCommandDescription(RedisCommand.HSET,key);
         }

         @Override
         public String getKeyFromData(Tuple3<String, Integer, String> key) {
             return key.f0;
         }

         @Override
         public String getValueFromData(Tuple3<String, Integer, String> value) {
             return value.f1+value.f2;
         }


     }
}
