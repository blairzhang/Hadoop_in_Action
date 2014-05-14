import java.io.IOException;
import java.util.Iterator;

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


public class KPIPV {
    
    public static class MapClass 
    	extends Mapper<Object, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text(); 
        public void map(Object key, Text value,
                        Context context ) throws IOException,
                        InterruptedException {
            KPI kpi = new KPI();
            kpi.parser(value.toString());
            if (kpi.isValid()){
				word.set(kpi.getRequest());
				context.write(word,one);
			}
        }
    }
    
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException,InterruptedException {
                           
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            context.write(key, new IntWritable(count));
        }
    }
    
    public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration();
        
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length != 2){
        	System.err.println("Usage: KPI <in> <out>");
        	System.exit(2);
        }
        
        Job job = new Job(conf, "KPIPV");
        job.setJarByClass(KPIPV.class);
        job.setMapperClass(MapClass.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        
        //job.setInputFormat(KeyValueTextInputFormat.class);
        //job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

