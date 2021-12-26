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
 *      在无序流上做时间分组统计时，乱序问题就影响到计算结果
 * 1、可以制定迟到的逻辑
 * 2、要有途径让Flink框架感知和贯彻执行用户对时间控制的业务需求
 *
 * watermark
 * 1、是一个时间戳
 * 2、在数据源位置发射
 * 3、也是和数据一起在算子之间传第
 * 4、触发窗口的计算，Long.MAX_VALUE值会告诉算子没有任何数据了
 *
 *
 * watermark定义：
 * 1、与业务时间保持一致的定义：Watermark（业务时间戳）
 * 2、业务乱序程度不超过1分钟的定义：Watermark（业务时间戳-60000）
 * 3、DataStream.assignTimestampsAndWatermarks(periodic/Puctuated)
 *
 * watermark传递：
 * 1、StreamElement：数据和watermark、状态都在一起传递
 * 2、多流：watermark保持单调递增，且多流汇总时，W=Min（input1，input2）
 * 3、单流多partition：同样也是保持取最小值
 *      【注意】：kafka数据倾斜会导致某一分区数据量特别少，进而导致整个流watermark停滞。可以设置一个默认等待时间
 *
 * @author: YouName
 * @create: 2021-11-21 23:04
 **/
public class WatermarkCase {


}
