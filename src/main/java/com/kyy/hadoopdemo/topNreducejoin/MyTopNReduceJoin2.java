package com.kyy.hadoopdemo.topNreducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;

/**
 * Created by kangyouyin on 2020/6/12.
 */
public class MyTopNReduceJoin2 {
    public static void main(String[] args) throws Exception {
        // init conf
        Configuration conf = new Configuration(true);
        conf.addResource("./cluster/core-site.xml");
        conf.addResource("./cluster/hdfs-site.xml");
        conf.addResource("./cluster/mapred-site.xml");
        conf.addResource("./cluster/yarn-site.xml");

        // get otherArgs
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(conf, args);
        String[] otherArgs = genericOptionsParser.getRemainingArgs();

        // init Job
        System.setProperty("HADOOP_USER_NAME", "root");
        Job job = Job.getInstance(conf);
        job.setJobName("MyTopNReduceJoin2");
        job.setJar("/Users/kangyouyin/IdeaProjects/hadoop-demo/target/hadoop-demo-0.0.1-SNAPSHOT.jar");
        job.setJarByClass(MyTopNReduceJoin2.class);

        // set input and output
        Path inputFile1 = new Path(otherArgs[0]);
        if (!inputFile1.getFileSystem(conf).exists(inputFile1)) {
            throw new Exception("Input file is not found : " + inputFile1.getName());
        }
        Path inputFile2 = new Path(otherArgs[1]);
        if (!inputFile2.getFileSystem(conf).exists(inputFile2)) {
            throw new Exception("Input file is not found : " + inputFile2.getName());
        }
        TextInputFormat.addInputPath(job, inputFile1);
        TextInputFormat.addInputPath(job, inputFile2);

        Path outputFile = new Path(otherArgs[2]);
        if (outputFile.getFileSystem(conf).exists(outputFile)) {
            outputFile.getFileSystem(conf).delete(outputFile, true);
        }
        TextOutputFormat.setOutputPath(job, outputFile);

        // map task
        job.setMapperClass(TMapper2.class);
        job.setMapOutputKeyClass(IntWritable.class);

        // reduce task
        // 错误： 原因？
        job.setReducerClass(TReducer2.class);
        // 正确
//        job.setReducerClass(TReducer3.class);

        job.waitForCompletion(true);
    }
}
