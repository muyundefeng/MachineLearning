package com.muyundefeng.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.canopy.Canopy;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.IOException;

/**
 * Created by lisheng on 17-4-18.
 */
public class Text2Cluster extends AbstractJob {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(),new Text2Cluster(),args);
    }
    public int run(String[] strings) throws Exception {
        addInputOption();
        addOutputOption();
        if(parseArguments(strings) == null)
            return -1;
        Path input = getInputPath();
        Path output = getOutputPath();
        Job job = new Job(getConf(),"text2cluster with input"+input.getName());
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapperClass(Text2ClusterMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ClusterWritable.class);
        job.setReducerClass(Text2ClusterReducer.class);

        job.setOutputValueClass(ClusterWritable.class);
        job.setOutputKeyClass(LongWritable.class);

        job.setJarByClass(Text2Cluster.class);
        FileInputFormat.addInputPath(job,input);
        SequenceFileOutputFormat.setOutputPath(job,output);
        if(!job.waitForCompletion(true))
            return -1;
        return 0;
    }

    private static int center = 1;
    static class Text2ClusterMapper extends Mapper<LongWritable,Text,LongWritable,ClusterWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str[] = value.toString().split(",");
            Vector vector = new RandomAccessSparseVector(str.length);
            for(int i =0;i<str.length;i++)
                vector.set(i,Double.parseDouble(str[i]));
            ClusterWritable clusterWritable = new ClusterWritable();
            Canopy canopy = new Canopy(vector,center++,new EuclideanDistanceMeasure());
            clusterWritable.setValue(canopy);
            context.write(key,clusterWritable);
        }
    }
    static class Text2ClusterReducer extends Reducer<LongWritable,ClusterWritable,LongWritable,ClusterWritable>{
        @Override
        protected void reduce(LongWritable key, Iterable<ClusterWritable> values, Context context) throws IOException, InterruptedException {
            for(ClusterWritable value:values)
                context.write(key,value);
        }
    }
}
