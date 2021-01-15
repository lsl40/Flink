package flink.stream;

import flink.pojo.Test;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: lsl
 * @Date: 2021/1/13 11:02
 * @Description:
 **/
public class EventTimeAndWaterMark {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 8888, '\n');

//        SingleOutputStreamOperator<Tuple3<String, Integer, String>> map = stringDataStreamSource
//                .map(new MapFunction<String, Tuple3<String, Integer, String>>() {
//            @Override
//            public Tuple3<String, Integer, String> map(String s) throws Exception {
//                String[] split = s.split(",");
//                return new Tuple3<>(split[0], Integer.valueOf(split[1]), split[2]);
//            }
//        });


        SingleOutputStreamOperator<Test> map = stringDataStreamSource.map(new MapFunction<String, Test>() {
            @Override
            public Test map(String s) throws Exception {
                String[] split = s.split(",");
                return new Test(split[0], Integer.valueOf(split[1]), split[2]);
            }
        });

        //------------------------------------设定 EVENT_TIME WATERMARK 侧输出流

        //后面{}不能省略
        OutputTag<Test> late = new OutputTag<Test>("late"){};

//        分配 Timestamps Watermarks
//        allowedLateness和Watermarks的区别:allowedLateness在窗口结束时候会触发计算,当有延迟数据时候会重新计算,WaterMark是将窗口计算整体延迟指定的时间
        SingleOutputStreamOperator<Test> window = map
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Test>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Test element) {
                //设定EVENT_TIME
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = null;
                try {
                    date = simpleDateFormat.parse(element.getTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                long ts = date.getTime();
                System.out.println(ts+"-------------");
                return ts;
            }
        })
                .keyBy(data -> data.getId())
//                .keyBy("id","count")
//                .countWindow(3);
//              timeWindow窗口起始便宜位置  默认为时间戳时间 % 窗口时间大小为整数时为第一个窗口结束的时间
                .timeWindow(Time.seconds(10))

//                window设定偏移量后起始窗口的位置 (默认为时间戳时间 - 偏移量时间) % 窗口时间大小为整数时为第一个窗口结束的时间
//                .window(TumblingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
//                //允许延迟五秒钟关闭窗口,当指定窗口结束时会输出一次结果，在允许迟到范围内的时间又来新数据时，会重计算并输出结果,当数据超出指定的超时时间,会进入侧输出流
//                .allowedLateness(Time.seconds(5))
//               侧输出流
                .sideOutputLateData(late)
                .sum("count");

        DataStream stlate = window.getSideOutput(late);

        window.print("window");
        stlate.print("late");

        env.execute();
    }
}
