package com.xinqing.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * MapReduce之Partitioner，决定MapTask的结果放入哪个ReduceTask执行。
 *
 * 该例子主要分为4个Partitioner，0处理xiaomi的数目，1处理huawei的数目，2处理iphone的数目，3处理其他的数目；并根据tab分割
 * xiaomi   400
 * huawei   800
 * xiaomi   330
 * iphone   500
 * iphone   400
 * huawei   600
 * vivo 400
 *
 *
 * 推荐extends Configured implements Tool
 *
 * Created by xuan on 2018/4/9
 */
public class PartitionerApp extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionerApp.class);

    @Override
    public int run(String[] args) throws Exception {
        // 创建Configuration
        Configuration conf = new Configuration();

        // 如果删除路径存在，则删除
        FileSystem fs = FileSystem.get(URI.create("hdfs://111.231.238.23:9000"), conf);
        LOG.warn("输出路径{}如果存在，则会被自动删除！！！", args[1]);
        fs.deleteOnExit(new Path(args[1]));

        // 创建Job
        Job job = Job.getInstance(conf, "partitionerApp");

        // 设置job的处理类
        job.setJarByClass(WordCount.class);

        // 设置作业的输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 设置Map相关的参数（主类、输出的key/value类型）
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置Reduce相关参数（主类、输出的key/value类型）
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置job的Partitioner处理类
        job.setPartitionerClass(Partition.class);
        // 指定ReduceTask的个数，与自己Partitioner处理类的数目要一致
        job.setNumReduceTasks(4);

        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交作业，等待完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PartitionerApp(), args));
    }

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 一行数据
            String line = value.toString();
            String[] split = line.split("\t");
            context.write(new Text(split[0]), new LongWritable(Long.parseLong(split[1])));
        }
    }

    public static class Partition extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            switch (key.toString()) {
                case "xiaomi":
                    return 0;
                case "huawei":
                    return 1;
                case "iphone":
                    return 2;
                default:
                    return 3;
            }
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                // 累计数目
                sum += val.get();
            }
            // 通过上下文将结果输出
            context.write(key, new LongWritable(sum));
        }
    }

}
