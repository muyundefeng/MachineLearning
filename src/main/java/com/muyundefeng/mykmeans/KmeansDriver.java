package com.muyundefeng.mykmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KmeansDriver {

    /**
     * k-means algorithm program
     */
    private static final String temp_path = "hdfs://localhost:9000/user/hadoop/date/kmeans/temp_center/";
    private static final String dataPath = "hdfs://localhost:9000/user/hadoop/input/smallkmeansdata";
    private static final int iterTime = 300;//设置最大循环次数
    private static int iterNum = 1;//表示当前的循环次数
    private static final double threadHold = 0.01;//设置相关的阈值

    private static Log log = LogFactory.getLog(KmeansDriver.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();

        // set the centers data file
        Path centersFile = new Path("hdfs://localhost:9000/user/hadoop/input/centers");
        DistributedCache.addCacheFile(centersFile.toUri(), conf);//存放中心向量的缓存,mapper中的setUp函数就是接受的该路径中的数据

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: KmeansDriver <indatafile> ");
            System.exit(2);
        }
        Job job = new Job(conf, "kmeans job 0");
        job.setJarByClass(KmeansDriver.class);
        job.setMapperClass(KmeansM.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DataPro.class);
        job.setNumReduceTasks(1);
        job.setCombinerClass(KmeansC.class);
        job.setReducerClass(KmeansR.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(dataPath));
        FileOutputFormat.setOutputPath(job, new Path(temp_path + 0 + "/"));
        if (!job.waitForCompletion(true)) {
            System.exit(1); // run error then exit
        }
        //  do iteration
        boolean flag = true;
        while (flag && iterNum < iterTime) {
            Configuration conf1 = new Configuration();

            // set the centers data file
            //根据上面初始化的中心向量重新循环计算中心向量
            Path centersFile1 = new Path(temp_path + (iterNum - 1) + "/part-r-00000");  //  the new centers file
            DistributedCache.addCacheFile(centersFile1.toUri(), conf1);
            boolean iterflag = doIteration(conf1, iterNum);
            if (!iterflag) {
                log.error("job fails");
                System.exit(1);
            }
            //  set the flag based on the old centers and the new centers

            Path oldCentersFile = new Path(temp_path + (iterNum - 1) + "/part-r-00000");//原始的中心向量文件
            Path newCentersFile = new Path(temp_path + iterNum + "/part-r-00000");//设置新的中心向量存放路径
            FileSystem fs1 = FileSystem.get(oldCentersFile.toUri(), conf1);
            FileSystem fs2 = FileSystem.get(oldCentersFile.toUri(), conf1);
            if (!(fs1.exists(oldCentersFile) && fs2.exists(newCentersFile))) {
                log.info("the old centers and new centers should exist at the same time");
                System.exit(1);
            }
            String line1, line2;
            FSDataInputStream in1 = fs1.open(oldCentersFile);
            FSDataInputStream in2 = fs2.open(newCentersFile);
            InputStreamReader istr1 = new InputStreamReader(in1);
            InputStreamReader istr2 = new InputStreamReader(in2);
            BufferedReader br1 = new BufferedReader(istr1);
            BufferedReader br2 = new BufferedReader(istr2);
            double error = 0.0;//误差计算公式
            while ((line1 = br1.readLine()) != null && ((line2 = br2.readLine()) != null)) {
                String[] str1 = line1.split("\t");
                String[] str2 = line2.split("\t");
                for (int i = 0; i < str1.length; i++) {//平方差公式计算误差
                    error += (Double.parseDouble(str1[i]) - Double.parseDouble(str2[i])) * (Double.parseDouble(str1[i]) - Double.parseDouble(str2[i]));
                }
            }
            if (error < threadHold) {
                flag = false;
            }
            iterNum++;//产生300个文件
        }
        // the last job , classify the data
        Configuration conf2 = new Configuration();
        // set the centers data file
        Path centersFile2 = new Path(temp_path + (iterNum - 1) + "/part-r-00000");  //  the new centers file
        DistributedCache.addCacheFile(centersFile2.toUri(), conf2);
        lastJob(conf2, iterNum);
        System.out.println(iterNum);
    }

    /**
     * 不断循环相关的过程,直到满足条件
     * @param conf
     * @param iterNum
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static boolean doIteration(Configuration conf, int iterNum) throws IOException, ClassNotFoundException, InterruptedException {
        boolean flag = false;
        Job job = new Job(conf, "kmeans job" + " " + iterNum);
        job.setJarByClass(KmeansDriver.class);
        job.setMapperClass(KmeansM.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DataPro.class);
        job.setNumReduceTasks(1);
        job.setCombinerClass(KmeansC.class);
        job.setReducerClass(KmeansR.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(dataPath));
        FileOutputFormat.setOutputPath(job, new Path(temp_path + iterNum + "/"));
        flag = job.waitForCompletion(true);
        return flag;
    }

    public static void lastJob(Configuration conf, int iterNum) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job(conf, "kmeans job" + " " + iterNum);
        job.setJarByClass(KmeansDriver.class);
        job.setMapperClass(KmeansLastM.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
        //  job.setCombinerClass(KmeansC.class);
        job.setReducerClass(KmeansLastR.class);//reduce函数直接写入文件
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(dataPath));
        FileOutputFormat.setOutputPath(job, new Path(temp_path + iterNum + "/"));
        job.waitForCompletion(true);
    }

}

