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

public class Step2 {
    
    public static class MapClass 
    	extends Mapper<Object, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text(); 
        public void map(Object key, Text value,
                        Context context ) throws IOException,
                        InterruptedException {
            String[] list = value.toString().split("\\\t|,");
            for(int i = 1;i<list.length ; i++)
            {
                String item1 = list[i].split(":")[0];
                for(int j = 1; j<list.length;j++)
                {
                    String item2 = list[j].split(":")[0];
                    word.set(item1 + ":" + item2);
                    context.write(word,one);
                }
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
    
    public static void step2Run(Map<String, String> path) throws Exception { 
        Configuration conf = new Configuration();
        
        String input = path.get("Step2Input");
        String output = path.get("Step2Output");
        Job job = new Job(conf, "step2Run");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
