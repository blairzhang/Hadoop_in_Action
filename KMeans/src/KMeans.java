import java.io.IOException;
import java.util.StringTokenizer;
import java.io.ByteArrayInputStream;  
import java.io.ByteArrayOutputStream;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class KMeans {
    public static String[] centerlist;
    public static int k = 0;//K 个数
    public static class MapClass 
    	extends Mapper<LongWritable, Text, Text, Text> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text(); 
        public void map(LongWritable key, Text value,
                        Context context ) throws IOException,
                        InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());  
		    while(itr.hasMoreTokens())  
		    {  
		        String outValue = new String(itr.nextToken());
  
		        String[] list = outValue.replace("(", "").replace(")", "").split(",");  
		        String[] c = centerlist[0].replace("(", "").replace(")", "").split(",");  
		        float min = 0;  
		        int pos = 0;  
		        for(int i=0;i<list.length;i++)  
		        {  
		            min += (float) Math.pow((Float.parseFloat(list[i]) - Float.parseFloat(c[i])),2);  
		        }  
		        for(int i=0;i<centerlist.length;i++)  
		        {  
		            String[] centerStrings = centerlist[i].replace("(", "").replace(")", "").split(",");  
		            float distance = 0;  
		            for(int j=0;j<list.length;j++)  
		                distance += (float) Math.pow((Float.parseFloat(list[j]) - Float.parseFloat(centerStrings[j])),2);  
		            if(min>distance)  
		            {  
		                min=distance;  
		                pos=i;  
		            }  
		        }  
		        context.write(new Text(centerlist[pos]), new Text(outValue));  
		    }  
        }
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException,InterruptedException {
                           
            String outVal = "";  
        	int count=0;  
		    String center="";  
		    int length = key.toString().replace("(", "").replace(")", "").replace(":", "").split(",").length;  
		    float[] ave = new float[Float.SIZE*length];  
		    for(int i=0;i<length;i++)  
		        ave[i]=0;   
		    for(Text val:values)  
		    {  
		        outVal += val.toString()+" ";  
		        String[] tmp = val.toString().replace("(", "").replace(")", "").split(",");  
		        for(int i=0;i<tmp.length;i++)  
		            ave[i] += Float.parseFloat(tmp[i]);  
		        count ++;  
		    }  
		    for(int i=0;i<length;i++)  
		    {  
		        ave[i]=ave[i]/count;  
		        if(i==0)  
		            center += "("+ave[i]+",";  
		        else {  
		            if(i==length-1)  
		                center += ave[i]+")";  
		            else {  
		                center += ave[i]+",";  
		            }  
		        }  
		    }  
		    System.out.println(center);  
		    context.write(key, new Text(outVal+center));
        }
    }
    public static void CenterInitial(String strInPath,String[] strcen) throws IOException{
    	String[] list;  
        String inpath = strInPath; 
        Configuration conf = new Configuration(); //读取hadoop文件系统的配置  
        conf.set("hadoop.job.ugi", "hadoop,hadoop");   
        FileSystem fs = FileSystem.get(URI.create(inpath),conf); //FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统   
        FSDataInputStream in = null;   
        ByteArrayOutputStream out = new ByteArrayOutputStream();  
        try{   
           
            in = fs.open( new Path(inpath) );   
            IOUtils.copyBytes(in,out,50,false);  //用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上   
            list = out.toString().split("\n");  
        } finally {   
        	IOUtils.closeStream(in);
        	out.close();  
        }
        for(int i = 0;i < strcen.length;i++)
        {
        	strcen[i] = list[i];
        }
    	
    }
    public static float NewCenter(String strOutPath) throws IOException{
    	String[] list;
    	float should = Integer.MIN_VALUE ;
    	Configuration conf = new Configuration(); //读取hadoop文件系统的配置  
        conf.set("hadoop.job.ugi", "hadoop,hadoop");   
        FileSystem fs = FileSystem.get(URI.create(strOutPath + "/part-r-00000"),conf); //FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统   
        FSDataInputStream in = null;   
        ByteArrayOutputStream out = new ByteArrayOutputStream();  
        try{   
           
            in = fs.open( new Path(strOutPath + "/part-r-00000") );   
            IOUtils.copyBytes(in,out,50,false);  //用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上   
            list = out.toString().split("\n");  
        } finally {   
        	IOUtils.closeStream(in);
        	out.close();  
        }
        
        for(int i = 0;i < k;i++){
        	String[] l = list[i].replace("\t", " ").split(" ");
        	String[] oldcenter = l[0].replace("(", "").replace(")", "").split(",");	//原先的中心点
        	String[] finalcenter = l[l.length-1].replace("(", "").replace(")", "").split(",");//新的中心点
        	centerlist[i] = l[l.length-1];		//保存最新的中心点
        	float tmp = 0;
        	for(int j = 0; j< oldcenter.length;j++){
        		tmp+=Math.pow(Float.parseFloat(oldcenter[j]) - Float.parseFloat(finalcenter[j]),2 );
        	}
        	if(should <= tmp)
        		should = tmp;
        }
        System.out.println("New Center...");
    	return should;
    	
    }
    public static void main(String[] args) throws Exception { 

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        int times = 0;
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length != 3){
        	System.err.println("Usage: KMeans K <in> <out>");
        	System.exit(2);
        }
        k = Integer.parseInt(args[0]);
        String[] strcen = new String[k];
        centerlist = strcen;
        CenterInitial(args[1], centerlist);		//初始化中心
        double s = 0;
        double shold = 0.0001;
        do{
	        Job job = new Job(conf, "KMeans");
	        job.setJarByClass(KMeans.class);
	        job.setMapperClass(MapClass.class);
	        job.setReducerClass(Reduce.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        
        	FileSystem fs = FileSystem.get(conf);		//每次循环，都删除输出目录  
	        fs.delete(new Path(args[2]),true);
	        FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	        if(job.waitForCompletion(true))  
	        {  
	            s = NewCenter(args[2]); 
	            times++;
	        }
        }while(s> shold);
        System.out.println("Iterator: " + times);	//迭代次数，即重复计算中心点的次数
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
