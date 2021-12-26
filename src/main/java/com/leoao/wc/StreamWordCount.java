package com.leoao.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: Flink_01
 * @description:
 * @author: YouName
 * @create: 2021-09-20 21:46
 **/
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //从文件中读取数据
        //String inputPath = "D:\\IdeaProject\\Flink_01\\src\\main\\resources\\hello.txt";
        //DataStream<String> stringDataStream = env.readTextFile(inputPath);
        //netcat
        //基于数据流进行转换计算
        //用parameter tool 从程序启动参数中提取配置项

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //socket文本流读取数据
        DataStream<String>  stringDataStream = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = stringDataStream.flatMap(new WordCount.MyFlatMapper()).keyBy(0).sum(1).setParallelism(2);


        resultStream.print();

        //执行任务
        env.execute();
    }
}
