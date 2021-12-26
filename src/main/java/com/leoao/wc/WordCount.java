package com.leoao.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @program: Flink_01
 * @description:
 * @author: Bacon
 * @create: 2021-09-20 21:29
 **/
public class WordCount {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        String inputPath = "D:\\IdeaProject\\Flink_01\\src\\main\\resources\\hello.txt";
        DataSet<String> stringDataSet = env.readTextFile(inputPath);

        //对数据集进行处理,按空格分词，
        DataSet<Tuple2<String, Integer>> resultSet = stringDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);

        resultSet.print();

    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //按空格分词
            String[] words= s.split(" ");
            //遍历所有word，包成2元组输出
            for (String word : words){
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
