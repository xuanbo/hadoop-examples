package com.xinqing.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * MapReduce统计单词数目
 *
 * Created by xuan on 2018/4/3
 */
public class WordCountCombiner {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 一行数据
            String line = value.toString();
            // 分割成单词，每个单词的value赋值为1
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                // 通过上下文将结果输出，作为Reduce的输入
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (IntWritable val : values) {
                // 累计单词出现的次数
                sum += val.get();
            }
            // 通过上下文将结果输出
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        // 创建Configuration
        Configuration conf = new Configuration();

        // 创建Job
        Job job = Job.getInstance(conf, "wordCount");

        // 设置job的处理类
        job.setJarByClass(WordCountCombiner.class);

        // 设置作业的输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 设置Map相关的参数（主类、输出的key/value类型）
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Reduce相关参数（主类、输出的key/value类型）
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置job的Combiner处理类，其实逻辑上与Reduce是一样的，只不过在输出的时候，先在本地Reducer一哈
        job.setCombinerClass(Reduce.class);

        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交作业，等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}