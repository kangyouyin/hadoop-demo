package com.kyy.hadoopdemo.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * Created by kangyouyin on 2020/6/9.
 */
public class WordCount {


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

        //让框架知道是windows异构平台运行
//        conf.set("mapreduce.app-submission.cross-platform", "true");


        System.setProperty("HADOOP_USER_NAME", "root");
        Job job = Job.getInstance(conf);
        job.setJobName("WordCount");

        job.setJar("/Users/kangyouyin/IdeaProjects/hadoop-demo/target/hadoop-demo-0.0.1-SNAPSHOT.jar");
        job.setJarByClass(WordCount.class);

        Path inputFile = new Path("/data/word_count.txt");
//        Path inputFile = new Path(otherArgs[0]);
        TextInputFormat.addInputPath(job, inputFile);

        Path outputFile = new Path("/data/result");
//        Path outputFile = new Path(otherArgs[1]);
        TextOutputFormat.setOutputPath(job, outputFile);

        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);


        //        job.setNumReduceTasks(2);
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);

    }
}
