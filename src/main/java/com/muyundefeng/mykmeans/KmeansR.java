package com.muyundefeng.mykmeans;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansR extends Reducer<IntWritable, DataPro, NullWritable, Text> {
    private static int dimension = 0;

    private static Log log = LogFactory.getLog(KmeansC.class);

    // the main purpose of the sutup() function is to get the dimension of the original data
    @SuppressWarnings("Duplicates")
    public void setup(Context context) throws IOException, InterruptedException {
        URI[] caches =context.getCacheFiles();
        if (caches == null || caches.length <= 0) {
            log.error("center file does not exist");
            System.exit(1);
        }
        FileSystem fileSystem = FileSystem.get(caches[0],context.getConfiguration(),"hadoop");
        FSDataInputStream inputStream = fileSystem.open(new Path(caches[0]));
        String str1 = IOUtils.toString(inputStream);
        String str [] =str1.split("\\n")[0].split("\\t");
        dimension = str.length;

    }

    @SuppressWarnings("Duplicates")
    public void reduce(IntWritable key, Iterable<DataPro> values, Context context) throws InterruptedException, IOException {

        double[] sum = new double[dimension];
        int sumCount = 0;
        for (DataPro val : values) {
            String[] datastr = val.getCenter().toString().split("\t");
            //	String[] datastr=val.getCenter().toString().split("\\s+");
            sumCount += val.getCount().get();
            for (int i = 0; i < dimension; i++) {
                sum[i] += Double.parseDouble(datastr[i]);
            }
        }
        //  calculate the new centers
        //	double[] newcenter=new double[dimension];
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < dimension; i++) {
            sb.append(sum[i] / sumCount + "\t");//重新计算每个簇的中心值
            //	sb.append(sum[i]/sumCount+"\\s+");
        }
        context.write(null, new Text(sb.toString()));
    }
}
