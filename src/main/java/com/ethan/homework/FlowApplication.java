package com.ethan.homework;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowApplication {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long startTime = System.currentTimeMillis();

        if (CollectionUtils.size(args) == 0) {
            args = new String[]{"input/HTTP_20130313143750.dat", "output/result.txt"};
        }

        //1.获取配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.反射类
        job.setJarByClass(FlowApplication.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //3.Reduce输入、输出的K、V类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //4.数据的输入和输出的指定目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //5.提交job
        job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println("程序运行的时间为：" + (endTime - startTime));
    }
}