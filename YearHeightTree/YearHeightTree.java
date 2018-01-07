import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class YearHeightTree extends Configured implements Tool {


	@Override
	public int run(String[] arg0) throws Exception {
		try {
			System.out.println(Arrays.toString(arg0));
			Job job = new Job(getConf(), "YearHeightTree");
			job.setJarByClass(YearHeightTree.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
	
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
	
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
	
			FileInputFormat.addInputPath(job, new Path(arg0[0]));
			FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
	
			job.waitForCompletion(true);
		} catch (Exception exception){
			System.out.println(exception.getMessage());
		}
		return 0;
	}


	public static void main(String[] args) throws IOException {
		
		try {
			System.out.println(Arrays.toString(args));
		    int res = ToolRunner.run(new Configuration(), new YearHeightTree(), args);
		    
		    System.exit(res);
		} catch (Exception exception) {
			
		}
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	      
		  private final static IntWritable ONE = new IntWritable(1);

	      @Override
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	  	String line = value.toString();
				// Process of the current line
				Tree.main(line);
				// Emit count 1
				context.write(new Text("lines"), ONE);
		  }
	 }

	 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	      @Override
	      public void reduce(Text key, Iterable<IntWritable> values, Context context)
	              throws IOException, InterruptedException {
	         int sum = 0;
	         for (IntWritable val : values) {
	            sum += val.get();
	         }
	         context.write(key, new IntWritable(sum));
	      }
	 }


}
