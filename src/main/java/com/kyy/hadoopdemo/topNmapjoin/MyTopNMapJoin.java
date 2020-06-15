package com.kyy.hadoopdemo.topNmapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
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
public class MyTopNMapJoin {
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
        job.setJobName("MyTopNMapJoin");
        job.setJar("/Users/kangyouyin/IdeaProjects/hadoop-demo/target/hadoop-demo-0.0.1-SNAPSHOT.jar");
        job.setJarByClass(MyTopNMapJoin.class);

        // set input and output
        job.addCacheFile(new Path("/data/topn/dict/dict.txt").toUri());

        Path inputFile = new Path(otherArgs[0]);
        if (!inputFile.getFileSystem(conf).exists(inputFile)) {
            BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(new File("./data/date_location_wd.txt")));
            FSDataOutputStream fsDataOutputStream = inputFile.getFileSystem(conf).create(inputFile);
            IOUtils.copyBytes(inputStream, fsDataOutputStream, conf, true);
        }
        TextInputFormat.addInputPath(job, inputFile);

        Path outputFile = new Path(otherArgs[1]);
        if (outputFile.getFileSystem(conf).exists(outputFile)) {
            outputFile.getFileSystem(conf).delete(outputFile, true);
        }
        TextOutputFormat.setOutputPath(job, outputFile);

        // map task
        job.setMapperClass(TMapper.class);
        job.setMapOutputKeyClass(TKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setPartitionerClass(TPartitioner.class);
        job.setSortComparatorClass(TSortComparator.class);

        // reduce task
        job.setReducerClass(TReducer.class);
        job.setGroupingComparatorClass(TGroupingComparator.class);

        job.waitForCompletion(true);
    }
}
