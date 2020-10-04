import java.io.IOException;
import java.lang.reflect.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;


public class access {

  public static class Map extends
            Mapper<LongWritable, Text, Text, Text> {

	FSDataInputStream fsin1,fsin2;
 	private Text word = new Text();
	private int i=1;
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
	
	try{
	Configuration conf = context.getConfiguration();
	Path p2= new Path("hdfs:///user/hduser/project/input/grade.csv");
	FileSystem fs2 = p2.getFileSystem(conf);
	fsin1 = fs2.open(p2);


	//Configuration conf = context.getConfiguration();
	Path p3= new Path("hdfs:///user/hduser/project/hash/part-m-00002");
	//FileSystem fs2 = p2.getFileSystem(conf);
	FileSystem fs3 = p3.getFileSystem(conf);
	fsin2 = fs3.open(p3);
	BufferedReader br=new BufferedReader(new InputStreamReader(fsin2));

	String line1;
	String line=value.toString();
	String line2=br.readLine();
	while(line2!=null)
	{
		String [] arr = line2.split("\t");
		
		if(arr[0].equalsIgnoreCase(line))
		{

		fsin1.seek(Long.parseLong(arr[1]));	
		line1 = fsin1.readLine();
		word.set(line1);
		context.write(new Text(String.valueOf(i)), word);
		i++;
		line="";
		
		}
	line2=br.readLine();
	}
	
	}
	catch(IOException e){}			
			
        }
    }

    public static class Reduce extends
            Reducer<Text, Text, Text, Text> {

      
	public void reduce(Text key, Text values, Context context)throws IOException, InterruptedException
	{
 
	 context.write(key, values);
	}

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
       // conf.set("fs.defaultFS", "localhost:50070");
        Job job = new Job(conf);
        job.setJarByClass(access.class);
        job.setJobName("random access");
	job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
	job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	Path outputPath = new Path(args[1]);
	//MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, Map.class);
        //MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, Map1.class)
	 //MultipleInputs.addInputPath(job, new Path(args[2]),TextInputFormat.class, Map2.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
