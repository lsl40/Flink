package flink.table;

import flink.pojo.Test;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: lsl
 * @Date: 2021/1/26 9:49
 * @Description:
 **/
public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//       读取数据
//        DataStreamSource<String> dataStream = env.readTextFile("D:\\workspace\\test\\Flink\\src\\main\\resources\\text1.txt");

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888, '\n');

//        数据转换
        SingleOutputStreamOperator<Test> map = dataStream.map(new MapFunction<String, Test>() {
            @Override
            public Test map(String s) throws Exception {
                String[] split = s.split(",");
                return new Test(split[0], Integer.valueOf(split[1]), split[2]);
            }
        });
//基于流创建一张表
        Table table = tableEnv.fromDataStream(map);
//        调用tableAPI进行转换操作
        Table where = table.select("id,count").where("count>1");



//        sql  注意如果使用sql中的关键字为字段,使用时添加符号``
        tableEnv.createTemporaryView("test",table);

        String sql = "select * from `test` where `count` >=2";
        Table sqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(where, Row.class).print("tableAPI");
        tableEnv.toAppendStream(sqlTable, Test.class).print("sqlTable");


        env.execute();



    }
}
