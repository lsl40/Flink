package flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @Author: lsl
 * @Date: 2021/1/30 10:56
 * @Description:
 **/
public class KafkaPipeLine {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        连接kafka注册表
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(new Kafka()
                .version("0.11")
        .topic("easylife-test")
        .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"dev-hadoop-7:9092,dev-hadoop-8:9092")
        .property("zookeeper.connect","dev-hadoop-6:2181"))
                .withFormat(new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("count",DataTypes.BIGINT())
                        .field("time",DataTypes.STRING()))
                .createTemporaryTable("inputtable");




        Table table = tableEnv.sqlQuery("select id,`count`,`time` from inputtable where id = '001'");

//        建立kafka连接输出到不同的topic下

        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("easylife-out")
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"dev-hadoop-7:9092,dev-hadoop-8:9092")
                .property("zookeeper.connect","dev-hadoop-6:2181"))
                .withFormat(new Json())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("count",DataTypes.BIGINT())
                        .field("time",DataTypes.STRING()))
                .createTemporaryTable("outputtable");


        table.executeInsert("outputtable");

        tableEnv.execute("");


    }
}
