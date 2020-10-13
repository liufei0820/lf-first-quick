package com.lf.flink.source;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Classname StreamSourceDemo
 * @Date 2020/9/24 下午6:53
 * @Created by fei.liu
 */
public class StreamSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源，设置并行度为1
        DataStreamSource<Item> text = env.addSource(new MyStreamingSource()).setParallelism(1);

        SingleOutputStreamOperator<Item> item = text.map((MapFunction<Item, Item>) value -> value);
        // 打印结果
        item.print().setParallelism(1);
        String jobName = "user defined streaming source";
        env.execute(jobName);
    }
}
