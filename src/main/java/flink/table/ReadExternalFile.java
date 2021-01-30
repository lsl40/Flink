package flink.table;

import flink.pojo.Test;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @Author: lsl
 * @Date: 2021/1/27 9:38
 * @Description:
 **/
public class ReadExternalFile {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        创建表,连接外部系统,读取数据
//        读取文件数据
        String s= "D:\\workspace\\test\\Flink\\src\\main\\resources\\text1.txt";
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(new FileSystem().path(s))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("count",DataTypes.BIGINT())
                        .field("time",DataTypes.STRING()))
                .createTemporaryTable("input");

        Table input = tableEnv.from("input");
//        tableEnv.toAppendStream(input, Row.class).print();


//        table API
//        Table filter = input.select("id,count,time").filter("id='001'");
//        Table group = input.groupBy("id").select("id,count.count");



//        sql

        Table table = tableEnv.sqlQuery("select id,`count`,`time` from input where id = '001'");
//        Table tableGroup = tableEnv.sqlQuery("select id,count(id),sum(`count`) from input group by id");

//        tableEnv.toAppendStream(filter,Row.class).print("filter");
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
//        rowDataStream.print();
//        rowDataStream.writeAsText("D:\\workspace\\test\\Flink\\src\\main\\tmp\\out.txt");
//        rowDataStream.writeAsCsv("D:\\workspace\\test\\Flink\\src\\main\\tmp\\out.csv", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE,",","\n");
//        tableEnv.toRetractStream(tableGroup,Row.class).print("tableGroup");
//        tableEnv.toRetractStream(group,Row.class).print("group");


//        创建输出表TODO
        String outFile = "D:\\workspace\\test\\Flink\\src\\main\\tmp\\out.txt";
        tableEnv.connect(new FileSystem().path(outFile))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("count",DataTypes.BIGINT())
                        .field("time",DataTypes.STRING()))
                .createTemporaryTable("outtable");

//        Table outtable = tableEnv.from("outtable");xxxxxxxxx


//        table.insertInto("outtable");
        table.executeInsert("outtable");

        tableEnv.execute("1");



//        tableEnv.executeSql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
    }



}
