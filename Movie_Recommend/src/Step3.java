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

public class Step3 {
    
    public static class Map1 
    	extends Mapper<Object, Text, IntWritable, Text> {
        
        private IntWritable k = new IntWritable();
        private Text v = new Text(); 
        public void map(Object key, Text value,
                        Context context ) throws IOException,
                        InterruptedException {
            String[] list = value.toString().split("\\\t|,");
            for(int i = 1;i<list.length ; i++)
            {
                String[] vector = list[i].split(":");
                int nItemID = Integer.parseInt(vector[0]);
                k.set(nItemID);
                v.set(list[0] + ":" + vector[1]);
                context.write(k,v);
            }
        }
    }
    
    public static class Map2 
    	extends Mapper<Object, Text, Text, IntWritable > {
        
        private IntWritable v = new IntWritable();
        private Text k = new Text(); 
        public void map(Object key, Text value,
                        Context context ) throws IOException,
                        InterruptedException {
            String[] list = value.toString().split("\\\t|,");
            k.set(list[0]);
            v.set(Integer.parseInt(list[1]));
            context.write(k,v);
        }
    }

    public static void step3Run1(Map<String, String> path) throws Exception { 
        Configuration conf = new Configuration();
        
        String input = path.get("Step3Input1");
        String output = path.get("Step3Output1");
        Job job = new Job(conf, "step3Run1");
        job.setJarByClass(Step3.class);
        job.setMapperClass(Map1.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
    
    public static void step3Run2(Map<String, String> path) throws Exception { 
        Configuration conf = new Configuration();
        
        //String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        //if(otherArgs.length != 2){
        //	System.err.println("Usage: KPI <in> <out>");
        //	System.exit(2);
        //}
        String input = path.get("Step3Input2");
        String output = path.get("Step3Output2");
        Job job = new Job(conf, "step3Run2");
        job.setJarByClass(Step3.class);
        job.setMapperClass(Map2.class);
        //job.setCombinerClass(Reduce.class);
        //job.setReducerClass(Reduce.class);
        
        //job.setInputFormat(KeyValueTextInputFormat.class);
        //job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
