import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IntWritable;

public class Step1 {
    
    public static class MapClass 
    	extends Mapper<Object, Text, IntWritable, Text > {
        
        public void map(Object key, Text value,
                        Context context ) throws IOException,
                        InterruptedException {
            String[] list = value.toString().split(",");
			context.write(new IntWritable(Integer.parseInt(list[0])),new Text(list[1] + ":" +list[2]));
        }
    }
    
    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
    	
    	private Text value = new Text();
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context) throws IOException,InterruptedException {
        	StringBuilder sb = new StringBuilder();
            for (Text val : values) {
            	sb.append( "," + val.toString());
            }
            value.set(sb.toString().replaceFirst(",", ""));
            context.write(key, new Text(value));
        }
    }
    
    public static void step1Run(Map<String, String> path) throws Exception { 
        Configuration conf = new Configuration();
        
        String input = path.get("data");
        String output = path.get("Step1Output");
        Job job = new Job(conf, "step1Run");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
