package com.kyy.hadoopdemo.fof;

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
 * Created by kangyouyin on 2020/6/9.
 */
public class MyFof {

    //bin/hadoop command [genericOptions] [commandOptions]
    //    hadoop jar  ooxx.jar  ooxx   -D  ooxx=ooxx  inpath  outpath
    //  args :   2类参数  genericOptions   commandOptions
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        conf.addResource("./cluster/core-site.xml");
        conf.addResource("./cluster/hdfs-site.xml");
        conf.addResource("./cluster/yarn-site.xml");
        conf.addResource("./cluster/mapred-site.xml");

        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(conf, args);
        String[] otherArgs = genericOptionsParser.getRemainingArgs();

        System.setProperty("HADOOP_USER_NAME", "root");
        Job job = Job.getInstance(conf);
        job.setJobName("MyFof");

        job.setJar("/Users/kangyouyin/IdeaProjects/hadoop-demo/target/hadoop-demo-0.0.1-SNAPSHOT.jar");
        job.setJarByClass(MyFof.class);

        Path inputFile = new Path(otherArgs[0]);
        if (!inputFile.getFileSystem(conf).exists(inputFile)) {
            BufferedInputStream inputLocal = new BufferedInputStream(new FileInputStream(new File("./data/fof.txt")));
            FSDataOutputStream fsDataOutputStream = inputFile.getFileSystem(conf).create(inputFile);
            IOUtils.copyBytes(inputLocal, fsDataOutputStream, conf, true);
        }
        TextInputFormat.addInputPath(job, inputFile);

        Path outputFile = new Path(otherArgs[1]);
        if (outputFile.getFileSystem(conf).exists(outputFile)) {
            outputFile.getFileSystem(conf).delete(outputFile, true);
        }
        TextOutputFormat.setOutputPath(job, outputFile);

        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);

        job.waitForCompletion(true);
    }
}
