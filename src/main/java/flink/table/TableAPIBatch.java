package flink.table;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * @Author: lsl
 * @Date: 2021/1/14 14:10
 * @Description:
 **/
public class TableAPIBatch {
    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSource<String> read = env.readTextFile("D:\\workspace\\test\\Flink\\src\\main\\resources\\wc.txt");

//        TODO
//        DataSource<String> read = env.readTextFile("D:\\workspace\\test\\Flink\\src\\main\\resources\\text1.txt");

        FlatMapOperator<String, WC> input = read.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String s, Collector<WC> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(new WC(s1,1));
                }
            }
        });


//        MapOperator<String, WD> input = read.map(new MapFunction<String, WD>() {
//            @Override
//            public WD map(String s) throws Exception {
//                String[] split = s.split(",");
//                WD wd = new WD(split[0], Double.valueOf(split[1]), split[2]);
//                System.out.println(wd+"-------------------");
//                return wd;
//            }
//        });

//        input.print();
//
//        DataSet<WC> input = env.fromElements(
//                new WC("Hello", 1),
//                new WC("Ciao", 1),
//                new WC("Hello", 1));

        // register the DataSet as table "WordCount"
        tEnv.registerDataSet("WordCount", input, "word, frequency");
//        tEnv.registerDataSet("datawd", input, "key,wd,times");

        // run a SQL query on the Table and retrieve the result as a new Table
        Table table = tEnv.sqlQuery(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");
//                "SELECT key,SUM(wd) AS wd,substring(times,0,9) as times FROM datawd GROUP BY key,substring(times,0,9)");

        DataSet<WC> result = tEnv.toDataSet(table, WC.class);
//        DataSet<WD> result = tEnv.toDataSet(table, WD.class);

        result.print();
    }


    public static class WC {
        public String word;
        public long frequency;

        // public constructor to make it a Flink POJO
        public WC() {}

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return  word + ":" + frequency;
        }
    }

    public  static  class  WD {
        public String key;
        public Double wd;
        public String times;

        public WD() {
        }
        public WD(String key, Double wd, String times) {
            this.key = key;
            this.wd = wd;
            this.times = times;
        }
        @Override
        public String toString() {
            return "WD{" +
                    "key='" + key + '\'' +
                    ", wd=" + wd +
                    ", times='" + times + '\'' +
                    '}';
        }
    }
}
