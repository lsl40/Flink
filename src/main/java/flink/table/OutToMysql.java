//package flink.table;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.descriptors.Csv;
//import org.apache.flink.table.descriptors.FileSystem;
//import org.apache.flink.table.descriptors.Schema;
//
///**
// * @Author: lsl
// * @Date: 2021/1/30 14:32
// * @Description:
// **/
//public class OutToMysql {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
////        创建表,连接外部系统,读取数据
////        读取文件数据
//        String s= "D:\\workspace\\test\\Flink\\src\\main\\resources\\text1.txt";
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        tableEnv.connect(new FileSystem().path(s))
//                .withFormat(new Csv())
//                .withSchema(new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("count",DataTypes.BIGINT())
//                        .field("time",DataTypes.STRING()))
//                .createTemporaryTable("input");
//
////        Table input = tableEnv.from("input");
//
//        Table table = tableEnv.sqlQuery("select id,`count`,`time` from input where id = '001'");
//
//        String sinkDDL=
//                "create table outputtable (" +
//                        "id varchar(20)," +
//                        "`count` bigint(11)," +
//                        "`time` varchar(20)) " +
//                        "with(" +
//                        " 'connector.type' = 'jdbc',"+
//                        " 'connector.url' = 'jdbc:mysql://localhost:3306/test',"+
//                        " 'connector.table' = 'input',"+
//                        " 'connector.driver' = 'com.mysql.jdbc.Driver',"+
//                        " 'connector.username' = 'root',"+
//                        " 'connector.password' = '123456')";
//
//        tableEnv.sqlUpdate(sinkDDL);
//        table.insertInto("outputtable");
//        tableEnv.execute("");
//
//
//
//    }
//}
