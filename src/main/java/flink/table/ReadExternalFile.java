package flink.table;

import flink.pojo.Test;
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

    private static ConnectTableDescriptor connectTableDescriptor;
    private static ConnectTableDescriptor schema;

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
        Table filter = input.select("id,count,time").filter("id='001'");
//        Table group = input.groupBy("id").select("id,`count`");xxx



//        sql

        Table table = tableEnv.sqlQuery("select id,`count`,`time` from input where id = '002'");
        Table tableGroup = tableEnv.sqlQuery("select id,count(id),sum(`count`) from input group by id");

//        tableEnv.toAppendStream(filter,Row.class).print("filter");
//        tableEnv.toAppendStream(table,Row.class).print("table");
        tableEnv.toRetractStream(tableGroup,Row.class).print("tableGroup");
//        tableEnv.toAppendStream(group,Row.class).print("group");xxx

        env.execute();
    }
}
