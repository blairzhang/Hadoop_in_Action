import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step4_2 {

    public static class Step4_2_Mapper extends
    		Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = values.toString().split("\\\t|,");
            Text k = new Text(tokens[0]);
            Text v = new Text(tokens[1]+","+tokens[2]);
            context.write(k, v);
        }
    }

    public static class Step4_2_Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, Double> map = new HashMap<String, Double>();

            for (Text line : values) {
                String[] tokens = line.toString().split("\\\t|,");
                String itemID = tokens[0];
                Double score = Double.parseDouble(tokens[1]);
                
                 if (map.containsKey(itemID)) {
                     map.put(itemID, map.get(itemID) + score);// 矩阵乘法求和计算
                 } else {
                     map.put(itemID, score);
                 }
            }
            
            Iterator<String> iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String itemID = iter.next();
                double score = map.get(itemID);
                Text v = new Text(itemID + "," + score);
                context.write(key, v);
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
    	Configuration conf = new Configuration();
        String input = path.get("Step6Input");
        String output = path.get("Step6Output");

        Job job = new Job(conf,"Step4_2");
        job.setJarByClass(Step4_2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_2_Mapper.class);
        job.setReducerClass(Step4_2_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}