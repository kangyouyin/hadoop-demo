package com.kyy.hadoopdemo.wc2;

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
 * Created by kangyouyin on 2020/6/11.
 */
public class WordCountFrequency {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        conf.addResource("./cluster/core-site.xml");
        conf.addResource("./cluster/hdfs-site.xml");
        conf.addResource("./cluster/mapred-site.xml");
        conf.addResource("./cluster/yarn-site.xml");

        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(conf, args);
        String[] otherArgs = genericOptionsParser.getRemainingArgs();

        System.setProperty("HADOOP_USER_NAME", "root");
        Job job = Job.getInstance(conf);
        job.setJobName("WordCountFrequency");
        job.setJar("/Users/kangyouyin/IdeaProjects/hadoop-demo/target/hadoop-demo-0.0.1-SNAPSHOT.jar");
        job.setJarByClass(WordCountFrequency.class);

        Path inputFile = new Path(otherArgs[0]);
        if (!inputFile.getFileSystem(conf).exists(inputFile)) {
            BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(new File("./data/word_count.txt")));
            FSDataOutputStream fsDataOutputStream = inputFile.getFileSystem(conf).create(inputFile);
            IOUtils.copyBytes(inputStream, fsDataOutputStream, conf, true);
        }
        TextInputFormat.addInputPath(job, inputFile);

        Path outFile = new Path(otherArgs[1]);
        if (outFile.getFileSystem(conf).exists(outFile)) {
            outFile.getFileSystem(conf).delete(outFile, true);
        }
        TextOutputFormat.setOutputPath(job, outFile);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReduce.class);

        job.waitForCompletion(true);
    }


}
