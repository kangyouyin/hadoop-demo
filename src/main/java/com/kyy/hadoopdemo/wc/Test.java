package com.kyy.hadoopdemo.wc;

import java.util.StringTokenizer;

/**
 * Created by kangyouyin on 2020/6/10.
 */
public class Test {
    public static void main(String[] args) {
        String str = "hello world , kyy kak, ni hao";

//        StringTokenizer itr = new StringTokenizer(str);
//        while (itr.hasMoreTokens()) {
//            System.out.println(itr.nextToken());
//        }

        StringTokenizer itr2 = new StringTokenizer(str, ",");
        while (itr2.hasMoreTokens()) {
            System.out.println(itr2.nextToken());
        }

    }

}
