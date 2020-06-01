package com.kyy.hadoopdemo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Created by kangyouyin on 2020/5/31.
 */
public class TestHDFS {

    private Configuration conf;
    private FileSystem fs;

    @Before
    public void conn() throws Exception {
        conf = new Configuration(true);
//        fs = FileSystem.get(conf);
        // Permission denied: user=kangyouyin, access=WRITE, inode="/user":root
        // 方式一：配置环境变量 HADOOP_USER_NAME root
        // 方式二：直接设置
        fs = FileSystem.get(URI.create("hdfs://mycluster"), conf, "root");
    }

    @Test
    public void mkdir() throws Exception {
//        Path path = new Path("test"); // 创建在 /user/root 目录下
        Path path = new Path("/test"); // 创建在hdfs根目录下
        if (fs == null) {
            System.out.println("FileSystem is null");
            return;
        }
        System.out.println(path.getParent() + " ==== " + path.toUri());
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        fs.mkdirs(path);
        System.out.println("mkdir successfully !!");
    }

    @Test
    public void upload() throws Exception {
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));

        Path path = new Path("/test/out.txt");
        FSDataOutputStream output = fs.create(path);

        IOUtils.copyBytes(input, output, conf, true);
    }


    @Test
    public void getBlock() throws Exception {
        Path path = new Path("/test/out.txt");
        FileStatus fileStatus = fs.getFileStatus(path);
        //FileStatus{path=hdfs://mycluster/test/out.txt; isDirectory=false; length=21; replication=2; blocksize=134217728; modification_time=1590929766761; access_time=1590977232819; owner=root; group=supergroup; permission=rw-r--r--; isSymlink=false}
        System.out.println(fileStatus);
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation blockLocation : fileBlockLocations) {
            //0,21,node03,node01
            System.out.println(blockLocation);
        }
    }

    @Test
    public void readFile() throws Exception {
        Path path = new Path("/test/out.txt");
        // 读取数据： 假设有两个块的信息
        //        0,        1048576,         node04,node02  A
        //  1048576,        5403190,         node04,node03  B

        //        blk01: he
        //        blk02: llo kyy

        //计算向数据移动~！
        //其实用户和程序读取的是文件这个级别~！并不知道有块的概念~！
        FSDataInputStream in = fs.open(path);
        //面向文件打开的输入流  无论怎么读都是从文件开始读起~！除非seek选择开始读取的位置
        //计算向数据移动后，期望的是分治，只读取自己关心（通过seek实现），同时，具备距离的概念（优先和本地的DN获取数据--框架的默认机制）
        in.seek(3);
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
    }

    @Test
    public void readFileByByte() throws Exception {
        Path path = new Path("/test/out.txt");
        FSDataInputStream input = fs.open(path);

        byte[] bytes = new byte[1024];
        int len = -1;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while ((len = input.read(bytes)) != -1) {
            out.write(bytes, 0, len);
        }
        System.out.println(new String(out.toByteArray()));
        input.close();
        out.close();
    }

    @Test
    public void readFileByCharacter() throws Exception {
        Path path = new Path("/test/out.txt");
        FSDataInputStream input = fs.open(path);
        BufferedReader br = null;
        String line;
        try {
            br = new BufferedReader(new InputStreamReader(input));
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            br.close();
        }
    }

    @Test
    public void writeByByte() throws Exception {
        Path path = new Path("/test/out.txt");
        // 覆盖
//        FSDataOutputStream out = fs.create(path);
        // 追加
        FSDataOutputStream out = fs.append(path);
        out.write("\n".getBytes());
        out.write("ok mike".getBytes());
        out.close();
    }

    @Test
    public void fromFile1AppendToFile2() throws Exception {
        Path file1path = new Path("/test/out.txt");
        Path file2path = new Path("/test/res.txt");
        FSDataInputStream input = fs.open(file1path);

        if (!fs.exists(file2path)) {
            fs.createNewFile(file2path);
        }
        // 追加
        FSDataOutputStream out = fs.append(file2path);
        IOUtils.copyBytes(input, out, 4096, true);
    }


    @After
    public void close() throws Exception {
        fs.close();
    }
}
