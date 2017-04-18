package com.muyundefeng.mykmeans;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansM extends Mapper<LongWritable, Text, IntWritable, DataPro> {
    private static Log log = LogFactory.getLog(KmeansM.class);

    private double[][] centers;
    private int dimention_m;  //  this is the k,中心向量集的大小,也就是k的大小
    private int dimention_n;   //  this is the features,向量的维度


    static enum Counter {Fansy_Miss_Records}

    ;

    @SuppressWarnings("Duplicates")
    @Override
    public void setup(Context context) throws IOException, InterruptedException {//mapper函数相关地初始化
        URI[] caches = context.getCacheFiles();
        if (caches == null || caches.length <= 0) {
            log.error("center file does not exist");
            System.exit(1);
        }
        System.out.println(caches[0].toString()+"**************");
        FileSystem fileSystem = FileSystem.get(caches[0],context.getConfiguration(),"hadoop");
        FSDataInputStream inputStream = fileSystem.open(new Path(caches[0]));
        String str1 = IOUtils.toString(inputStream);
        System.out.println("form center content = "+str1);
        String lines[]= str1.split("\\n");
        List<ArrayList<Double>> temp_centers = new ArrayList<ArrayList<Double>>();
        ArrayList<Double> center = null;//相当一个向量
        //  get the file data
        for (int j =0;j<lines.length;j++) {
            center = new ArrayList<Double>();
            String[] str = lines[j].split("\t");
            //	String[] str=line.split("\\s+");
            for (int i = 0; i < str.length; i++) {
                center.add(Double.parseDouble(str[i]));
                //	center.add((double)Float.parseFloat(str[i]));
            }
            temp_centers.add(center);//创建向量集
        }

        //  fill the centers
        @SuppressWarnings("unchecked")
        ArrayList<Double>[] newcenters = temp_centers.toArray(new ArrayList[]{});
        dimention_m = temp_centers.size();
        dimention_n = newcenters[0].size();
        centers = new double[dimention_m][dimention_n];//数组链表转化为二维数组,存放中心向量集
        for (int i = 0; i < dimention_m; i++) {
            Double[] temp_double = newcenters[i].toArray(new Double[]{});
            for (int j = 0; j < dimention_n; j++) {
                centers[i][j] = temp_double[j];
                //		System.out.print(temp_double[j]+",");
            }
            //	System.out.println();
        }
    }


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\t");//此时的键值对并不是中心向量文件中的向量值,是真正的数据文件
        //	String[] values=value.toString().split("\\s+");
        if (values.length != dimention_n) {
            context.getCounter(Counter.Fansy_Miss_Records).increment(1);//创建用户自定义的计数器,统计损坏的数据
            return;
        }
        double[] temp_double = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            temp_double[i] = Double.parseDouble(values[i]);
        }
        //  set the index
        double distance = Double.MAX_VALUE;
        double temp_distance = 0.0;
        int index = 0;
        for (int i = 0; i < dimention_m; i++) {
            //查看该条记录(键值对)是属于哪一个中心向量所引导的簇
            double[] temp_center = centers[i];//
            temp_distance = getEnumDistance(temp_double, temp_center);
            if (temp_distance < distance) {
                index = i;
                distance = temp_distance;
            }
        }
        DataPro newvalue = new DataPro();
        newvalue.set(value, new IntWritable(1));
        //	System.out.println("the map out:"+index+","+value);
        context.write(new IntWritable(index), newvalue);//写入文件,所属于向量的索引值,以及属于该中心向量的值
    }

    /**
     * 距离计算公式,可以进行
     * @param source
     * @param other
     * @return
     */
    @SuppressWarnings("Duplicates")
    public static double getEnumDistance(double[] source, double[] other) {  //  get the distance
        double distance = 0.0;
        if (source.length != other.length) {
            return Double.MAX_VALUE;
        }
        for (int i = 0; i < source.length; i++) {
            distance += (source[i] - other[i]) * (source[i] - other[i]);
        }
        distance = Math.sqrt(distance);
        return distance;
    }
}

