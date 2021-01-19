package flink.processFunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: lsl
 * @Date: 2021/1/18 16:49
 * @Description:
 **/
public class ProcessOnTimer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 8888, '\n');


        SingleOutputStreamOperator<Tuple3<String, Integer, String>> map = stringDataStreamSource.map(new MapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple3<>(split[0], Integer.valueOf(split[1]), split[2]);
            }
        });
        map.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, String>>(Time.seconds(0)) {
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
                System.out.println(ts+"-------------");
                return ts;
            }
        }).keyBy(data->data.f0)
//                .timeWindow(Time.seconds(10))
                .process(new MyProcessFunction()).print();
//        .process(new MyWindowProcessFunction()).print();

        env.execute();

    }

    public static class MyWindowProcessFunction extends ProcessWindowFunction<Tuple3<String, Integer, String>,String,String, TimeWindow>{
        @Override
        public void process(String s, Context context, Iterable<Tuple3<String, Integer, String>> elements, Collector<String> out) throws Exception {
            System.out.println(s);

            for (Tuple3<String, Integer, String> element : elements){
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = null;
                try {
                    date = simpleDateFormat.parse(element.f2);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                long ts = date.getTime();


                out.collect(ts +"--"+s);
            }
        }
    }


    public static class MyProcessFunction extends KeyedProcessFunction<String,Tuple3<String,Integer,String>,String>{
        @Override
        public void processElement(Tuple3<String, Integer, String> value, Context ctx, Collector<String> out) throws Exception {


            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = null;
            try {
                date = simpleDateFormat.parse(value.f2);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            long ts = date.getTime();


//            ctx.timerService().registerEventTimeTimer(ts+5000L);  TODO
//            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+5000L);
            ctx.timerService().registerProcessingTimeTimer(ts);

            System.out.println(ctx.timerService().currentProcessingTime()+"======================="+ts);
            out.collect(value.f2);
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            String currentKey = ctx.getCurrentKey();

            System.out.println(currentKey+"==================="+timestamp);

            System.out.println(timestamp+"----定时触发----");
        }



    }


}
