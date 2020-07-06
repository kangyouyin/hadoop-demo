package com.kyy.hadoopdemo.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by kangyouyin on 2020/7/6.
 */
public class TuoMin extends UDF {

    public Text evaluate(final Text s) {

        if (s == null) {
            return null;
        }
        String str = s.toString().substring(0, 1) + "-kyy";
        return new Text(str);
    }
}
