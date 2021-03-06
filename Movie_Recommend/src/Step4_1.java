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

public class Step4_1 {

    public static class Step4_1_Mapper extends
    		Mapper<Object, Text, Text, Text> {

        private String flag;// A同现矩阵 or B评分矩阵

        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的数据集
            // System.out.println(flag);
        }

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = values.toString().split("\\\t|,");

            if (flag.equals("step3_2")) {// 同现矩阵
                String[] v1 = tokens[0].split(":");
                String itemID1 = v1[0];
                String itemID2 = v1[1];
                String num = tokens[1];

                Text k = new Text(itemID1);
                Text v = new Text("A:" + itemID2 + "," + num);

                context.write(k, v);

            } else if (flag.equals("step3_1")) {// 评分矩阵
                String[] v2 = tokens[1].split(":");
                String itemID = tokens[0];
                String userID = v2[0];
                String pref = v2[1];

                Text k = new Text(itemID);
                Text v = new Text("B:" + userID + "," + pref);

                context.write(k, v);
            }
        }
    }

    public static class Step4_1_Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();

            for (Text line : values) {
                String val = line.toString();

                if (val.startsWith("A:")) {
                    String[] kv = val.substring(2).split("\\\t|,");
                    mapA.put(kv[0], kv[1]);

                } else if (val.startsWith("B:")) {
                    String[] kv = val.substring(2).split("\\\t|,");
                    mapB.put(kv[0], kv[1]);
                }
            }

            double result = 0;
            Iterator iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = (String) iter.next();// itemID
                int num = Integer.parseInt(mapA.get(mapk));
                Iterator iterb = mapB.keySet().iterator();
                while (iterb.hasNext()) {
                    String mapkb = (String) iterb.next();// userID
                    double pref = Double.parseDouble(mapB.get(mapkb));
                    result = num * pref;// 矩阵乘法相乘计算

                    Text k = new Text(mapkb);
                    Text v = new Text(mapk + "," + result);
                    context.write(k, v);
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
    	Configuration conf = new Configuration();
        String input1 = path.get("Step5Input1");
        String input2 = path.get("Step5Input2");
        String output = path.get("Step5Output");

        Job job = new Job(conf,"Step4_1");
        job.setJarByClass(Step4_1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_1_Mapper.class);
        job.setReducerClass(Step4_1_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}