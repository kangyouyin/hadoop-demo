package com.kyy.hadoopdemo.debug;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by kangyouyin on 2020/6/9.
 */
public class MyFof {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.default.name", "local");

        Job job = Job.getInstance(conf);
        job.setJobName("MyFof");
        job.setJarByClass(MyFof.class);

        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        FileInputFormat.setInputPaths(job, new Path("./data/fof.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./data/output"));

        job.waitForCompletion(true);
    }
}
