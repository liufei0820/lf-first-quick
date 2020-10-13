package com.lf.flink.distributecache;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @Classname DistributedCacheTest
 * @Date 2020/9/27 下午9:26
 * @Created by fei.liu
 */
public class DistributedCacheTest {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 1.注册一个文件，可以使用Hdfs上的文件，也可以是本地文件进行测试
        env.registerCachedFile("/Users/apple/Downloads/distributedcache.txt", "distributedCache");
        DataSource<String> data = env.fromElements("Linea", "Lineb", "Linec", "Lined");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 使用缓存文件
                File myFile = getRuntimeContext().getDistributedCache().getFile("distributedCache");
                List<String> lines = FileUtils.readLines(myFile);
                for (String line : lines) {
                    this.dataList.add(line);
                    System.err.println("分布式缓存为:" + line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                // 在这里就可以使用dataList
                System.err.println("使用dataList：" + dataList + "-------" + value);
                // 业务逻辑
                return dataList + ": " + value;
            }
        });

        result.printToErr();

    }
}
