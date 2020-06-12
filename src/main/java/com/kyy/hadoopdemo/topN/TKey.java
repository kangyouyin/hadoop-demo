package com.kyy.hadoopdemo.topN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kangyouyin on 2020/6/12.
 */
public class TKey implements WritableComparable<TKey> {

    private int year;
    private int month;
    private int day;
    private int wd;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getWd() {
        return wd;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }

    @Override
    public int compareTo(TKey other) {
        int c1 = Integer.compare(this.year, other.year);
        if (c1 == 0) {
            int c2 = Integer.compare(this.month, other.month);
            if (c2 == 0) {
                return Integer.compare(this.day, other.day);
            }
            return c2;
        }
        return c1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeInt(this.month);
        out.writeInt(this.day);
        out.writeInt(this.wd);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.wd = in.readInt();
    }

    @Override
    public String toString() {
        return this.year + "-" + this.month + "-" + this.day + "-" + this.wd;
    }
}
