package flink.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: lsl
 * @Date: 2021/1/26 14:22
 * @Description:
 **/
public class TableEnvs {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

//        基于老版本planner的流处理
        EnvironmentSettings oldStream = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamEnv, oldStream);
//        基于老版本planner的批处理
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);

//        基于blink的流处理
        EnvironmentSettings blinkStream = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment streamTableEnvironment1 = StreamTableEnvironment.create(streamEnv, blinkStream);

//        基于blink的批处理
        EnvironmentSettings blinkbatch = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(blinkbatch);



    }
}
