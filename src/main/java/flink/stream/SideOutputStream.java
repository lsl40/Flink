package flink.stream;

import flink.pojo.Test;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: lsl
 * @Date: 2021/1/19 14:34
 * @Description:
 **/
public class SideOutputStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 8888, '\n');
        SingleOutputStreamOperator<Test> map = stringDataStreamSource.map(new MapFunction<String, Test>() {
            @Override
            public Test map(String s) throws Exception {
                String[] split = s.split(",");
                return new Test(split[0], Integer.valueOf(split[1]), split[2]);
            }
        });


        OutputTag<Test> low = new OutputTag<Test>("low"){};
        SingleOutputStreamOperator<Test> high = map.process(new MyProcess());
        high.print("high");
        high.getSideOutput(low).print("low");
        env.execute();

    }


    public static class MyProcess  extends ProcessFunction<Test, Test>{
        OutputTag<Test> low = new OutputTag<Test>("low"){};
        @Override
        public void processElement(Test value, Context ctx, Collector<Test> out) throws Exception {
            if(value.getCount()>30){
                out.collect(value);
            }else{
                ctx.output(low,value);
            }
        }
    }
}
