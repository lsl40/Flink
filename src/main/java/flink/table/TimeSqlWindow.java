package flink.table;

import flink.pojo.Test;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: lsl
 * @Date: 2021/2/1 14:21
 * @Description:
 **/
public class TimeSqlWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888, '\n');
//        DataStreamSource<String> dataStream = env.readTextFile("D:\\workspace\\test\\Flink\\src\\main\\resources\\text1.txt");

        SingleOutputStreamOperator<Test> map = dataStream.map(new MapFunction<String, Test>() {
            @Override
            public Test map(String s) throws Exception {
                String[] split = s.split(",");
                return new Test(split[0], Integer.valueOf(split[1]), split[2]);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Test>(Time.seconds(0)) {
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
                System.out.println(ts + "-------------"+element.getTime());
                return ts;
            }
        });


        Table table = tableEnv.fromDataStream(map, "id,count,time,rt.rowtime,pt.proctime");



        tableEnv.createTemporaryView("test",table);

        // SQL 查询一个已经注册的表
        String sql = "select * from `test` where `count` >=2";
        // SQL 查询一个未注册的表
        String sql1 = "select * from "+table+" where `count` >=2";
        Table sqlTable = tableEnv.sqlQuery(sql);

//=====================================================滚动窗口操作
//        eventtime
//        Table tumbleRowtime = sqlTable
//                .window(Tumble.over("5.second").on("rt").as("tw"))
//                .groupBy("id,tw")
//                .select("id,id.count,count.avg,count.sum,tw.start,tw.end");
//        proctime
        Table tumbleProctime = sqlTable
                .window(Tumble.over("5.second").on("pt").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,count.avg,count.sum,tw.start,tw.end");

//        sql  tumble(rt,interval '10' second)
        Table tumbleRowtime = tableEnv.sqlQuery("select id,count(id),sum(`count`),avg(`count`) " +
                ",tumble_START(rt,interval '10' second) " +
                ",tumble_END(rt,interval '10' second) " +
                "from test  group by id ,tumble(rt,interval '10' second)");



        Table tumbleRowtimeNum = tableEnv.sqlQuery(
                "select * from ("+
                "select id,`count`,`time` " +
                ",ROW_NUMBER() OVER (PARTITION BY id ORDER BY `count` desc) as row_num " +
                " from test"+
                ")  AS t where row_num<=2 "
                );

//                ",tumble_START(rt,interval '10' second) " +
//                ",tumble_END(rt,interval '10' second) " +



//        countWindow TODO
//        Table count = sqlTable xxxxxxxxxxxx
//                .window(Tumble.over("5.rows").on("rt").as("tw"))
//                .groupBy("id,tw")
//                .select("id,id.count,count.avg,count.sum,tw.start,tw.end");

//        tableEnv.toAppendStream(tumbleRowtime, Row.class).print("tumbleRowtime");
        tableEnv.toRetractStream(tumbleRowtimeNum, Row.class).print("tumbleRowtimeNum");

//        tableEnv.toAppendStream(tumbleProctime, Row.class).print("tumbleProctime");

//=====================================================滚动窗口操作
        //        eventtime
        Table slideRowtime = sqlTable
                .window(Slide.over("10.second").every("5.second").on("rt").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,count.avg,count.sum,tw.start,tw.end");

//      HOP(rt ,interval '5' second,interval '10' second)  第一个参数为滑动步长,第二个参数为窗口大小
        Table hopRowtime = tableEnv.sqlQuery("select id,count(id),sum(`count`),avg(`count`) " +
                ",HOP_START(rt ,interval '5' second,interval '10' second) " +
                ",HOP_END(rt ,interval '5' second,interval '10' second) " +
                " from test  group by id ,HOP(rt ,interval '5' second,interval '10' second)");
//        tableEnv.toAppendStream(hopRowtime, Row.class).print("hopRowtime");

//        tableEnv.toAppendStream(slideRowtime, Row.class).print("slideRowtime");



//=====================================================会话窗口操作

        Table session = sqlTable
                .window(Session.withGap("3.second").on("rt" ).as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,count.avg,count.sum,tw.start,tw.end");


        Table sessionRowtime = tableEnv.sqlQuery("select id,count(id),sum(`count`),avg(`count`) " +
                ",SESSION_START(rt,interval '10' second) " +
                ",SESSION_END(rt,interval '10' second) " +
                "from test  group by id ,SESSION(rt,interval '10' second)");

//        tableEnv.toAppendStream(session, Row.class).print("session");
//        tableEnv.toAppendStream(sessionRowtime, Row.class).print("sessionRowtime");

        env.execute();
    }
}
