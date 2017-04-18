package com.muyundefeng.cluster;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by lisheng on 17-4-17.
 */
public class Text2VectorWritableMapper extends Mapper<LongWritable,Text,LongWritable,VectorWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str[] = value.toString().split(",");
        Vector vector = new RandomAccessSparseVector(str.length);
        for(int i =0;i<str.length;i++) {
            vector.set(i, Double.parseDouble(str[i]));
        }
        VectorWritable vw = new VectorWritable(vector);//序列化vector类
        context.write(key,vw);
    }
}
