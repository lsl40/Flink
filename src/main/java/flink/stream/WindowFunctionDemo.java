package flink.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: lsl
 * @Date: 2021/1/9 10:17
 * @Description:
 **/
public class WindowFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 8888, '\n');
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> map = stringDataStreamSource.map(new MapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple3<>(split[0], Integer.valueOf(split[1]), "split[2]");
            }
        });


//        WindowedStream<Tuple3<String, Integer, String>, String, TimeWindow> window= map.keyBy(data -> data.f0)
////                .countWindow(3);
//                .timeWindow(Time.seconds(10));




        //------------------------------------增量窗口函数
//        ReduceFunction
//        window.reduce(new ReduceFunction<Tuple3<String, Integer, String>>() {
//            @Override
//            public Tuple3<String, Integer, String> reduce(Tuple3<String, Integer, String> old, Tuple3<String, Integer, String> news) throws Exception {
//                return new Tuple3<String, Integer, String>(news.f0, news.f1+old.f1,news.f0+old.f0);
//            }
//        }).print();


//        AggregateFunction  算窗口f1平均值
//        window.aggregate(new AggregateFunction<Tuple3<String, Integer, String>, Tuple2<Double,Integer>, Double>() {
//
//            @Override
//            public Tuple2<Double, Integer> createAccumulator() {
//                return new Tuple2<Double, Integer>(0.0,0);
//            }
//
//            @Override
//            public Tuple2<Double, Integer> add(Tuple3<String, Integer, String> in, Tuple2<Double, Integer> out) {
//                return new Tuple2<Double, Integer>(out.f0+in.f1,out.f1+1);
//            }
//
//            @Override
//            public Double getResult(Tuple2<Double, Integer> out) {
//                return out.f0 / out.f1;
//            }
//
//            @Override
//            public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> acc1) {
//                return null;
//            }
//        }).print();





        //------------------------------------全量窗口函数


//        WindowFunction
//        window.apply(new WindowFunction<Tuple3<String, Integer, String>, Object, String, TimeWindow>() {
//            @Override
//            public void apply(String s, TimeWindow window, Iterable<Tuple3<String, Integer, String>> input, Collector<Object> out) throws Exception {
//                Iterator<Tuple3<String, Integer, String>> iterator = input.iterator();
//                if(iterator.hasNext()){
//                    Tuple3<String, Integer, String> next = iterator.next();
//                    System.out.println(next.toString());
//                }
//                out.collect(window.getStart()+"==="+window.getEnd());
//            }
//        }).print();




//        window.process(new ProcessWindowFunction<Tuple3<String, Integer, String>, Object, String, TimeWindow>() {
//            @Override
//            public void process(String s, Context context, Iterable<Tuple3<String, Integer, String>> elements, Collector<Object> out) throws Exception {
//                System.out.println(s);
//                long l = context.currentWatermark();
//                TimeWindow window1 = context.window();
//                long start = window1.getStart();
//                long end = window1.getEnd();
//            }
//        });




        env.execute();

    }
}
