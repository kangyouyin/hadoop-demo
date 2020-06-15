package com.kyy.hadoopdemo.topNreducejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by kangyouyin on 2020/6/12.
 */
public class TMapper2 extends Mapper<Object, Text, IntWritable, Text> {

    private final static String DW_PREFIX = "X:";
    private final static String LOCATION_PREFIX = "Y:";

    private final static IntWritable mkey = new IntWritable();
    private final static Text mval = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //1	X:1970-8-8&32
        //2	X:1970-8-23&23
        //1	beijing
        //2	shanghai
        //3	guangzhou
        String[] str = value.toString().split("\t");
        String valTmp = str[1];
        if (!valTmp.startsWith(DW_PREFIX)) {
            valTmp = LOCATION_PREFIX + valTmp;
        }
        mkey.set(Integer.parseInt(str[0]));
        mval.set(valTmp);
        context.write(mkey, mval);
    }
}
