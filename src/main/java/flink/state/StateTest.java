package flink.state;

import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author: lsl
 * @Date: 2021/1/15 9:54
 * @Description:
 **/
public class StateTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 8888, '\n');

        SingleOutputStreamOperator<Tuple3<String, Integer, String>> map = stream.map(new MapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(String s) throws Exception {

                String[] split = s.split(",");
                return new Tuple3<>(split[0], Integer.valueOf(split[1]), split[2]);
            }
        });

        SingleOutputStreamOperator<Integer> map1 = map.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, String>>(org.apache.flink.streaming.api.windowing.time.Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<String, Integer, String> element) {

                //设定EVENT_TIME
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = null;
                try {
                    date = simpleDateFormat.parse(element.f2);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                long ts = date.getTime();
                System.out.println(ts + "-------------");
                return ts;
            }
        }).keyBy(data -> data.f0).map(new MyRichMapFunction());

//                ReduceFunction of reduce can not be a RichFunction.
//                timeWindow(Time.seconds(10)).reduce(new MyRichReduceFunction());
        map1.print("--------------");
        env.execute();
    }


    public static  class MyRichMapFunction extends RichMapFunction<Tuple3<String, Integer, String>,Integer> {

        private ValueState<Integer> keyState;
        private ListState<String> listState;
        private MapState<String, String> mapState;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //设置ttl更新策略为创建和写，直观作用为如果一个key（例如20200101 01）1个小时内没有写入的操作，只有读的操作，那么这个key将被标记为超时
            //值得注意的是，MapState ListState这类集合state，超时机制作用在每个元素上，也就是每个元素的超时是独立的
            //设置ValueState的TTL超时时间为5s，超过五秒后到期自动清除状态
            //注意  不是没5秒清除一次状态,是持续15秒没有数据进来重新计算

//            StateTtlConfig build = StateTtlConfig.newBuilder(Time.seconds(5))
//                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                    .build();

            ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor("key-state", Integer.class);
            ListStateDescriptor<String> stringListStateDescriptor = new ListStateDescriptor<>("list-state", String.class);
            MapStateDescriptor<String, String> stringIntegerMapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, String.class);

//            valueStateDescriptor.enableTimeToLive(build);
            keyState = getRuntimeContext().getState(valueStateDescriptor);
            listState = getRuntimeContext().getListState(stringListStateDescriptor);
            mapState = getRuntimeContext().getMapState(stringIntegerMapStateDescriptor);
//            getRuntimeContext().getReducingState()
        }

        @Override
        public Integer map(Tuple3 tuple3) throws Exception {

//            mapState.put(tuple3.f0.toString(),Integer.valueOf(tuple3.f1.toString()));


            String s1 = mapState.get(tuple3.f0.toString());
            String s2 = mapState.get(tuple3.f1.toString());

            mapState.put(tuple3.f0.toString(),tuple3.f1.toString());
            mapState.put(tuple3.f1.toString(),tuple3.f2.toString());


//            System.out.println(s1+"----------"+s2);

//            System.out.println(integer+"-----------------map");

            Integer value = keyState.value();

            listState.add(tuple3.f0.toString()+tuple3.f1.toString()+tuple3.f2.toString());

            Iterable<String> ls = listState.get();

            Iterator<String> iterator = ls.iterator();

            while (iterator.hasNext()){
                String next = iterator.next();
                System.out.println(next+"=========liststate");
            }



            if(value == null){
                value = 1;
            }else{
                value++;
            }
            keyState.update(value);
            Iterable<String> strings = listState.get();
//            for (String string : strings) {
//                System.out.println(string+"-----------------list");
//            }
            return value;
        }
    }
}
