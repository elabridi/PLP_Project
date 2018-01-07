package edu.centrale.plp.pagerank;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.math3.util.OpenIntToDoubleHashMap.Iterator;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {
	
	public static HashMap<Integer,Node> map = new HashMap<Integer,Node>();
	
	public class Node{
		private double pageRank;
		
		private List<Integer> adjacentList = null;
		
		private int nid;
		
		public Node(){
			
		}
		
		public Node(int nid){
			this.nid = nid;
		}
		
		public double getPageRank(){
			return this.pageRank;
		}
		
		public void setPageRank(double pr){
			this.pageRank = pr;
		}
		
		public List<Integer> getAdjacentList(){
			return this.adjacentList;
		}
		
		public Integer getNid(){
			return this.nid;
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		try {
			System.out.println(Arrays.toString(arg0));
			Job job = new Job(getConf(), "WordCount");
			job.setJarByClass(PageRank.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
	
			job.setMapperClass(Map.class);
			//job.setCombinerClass(Reduce.class); //initial combiner, before in-mapper combining
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

	public static void main(String[] args) {
		try {
			System.out.println(Arrays.toString(args));
		    int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		    
		    System.exit(res);
		} catch (Exception exception) {
			
		}

	}
	
	public static class Map extends Mapper<LongWritable, Text, Integer, Object> {
	      HashMap<String,Integer> hmap = new HashMap<>();
	      

	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	Configuration conf = new Configuration();
		    String infile = "soc-Epinions1.txt";
    	    Path ofile = new Path(infile);
    	    FileSystem fs = ofile.getFileSystem(conf);
	  		InputStream in = new BufferedInputStream(fs.open(ofile));
	  		try{
				InputStreamReader isr = new InputStreamReader(in);
				BufferedReader br = new BufferedReader(isr);
				
				// read line by line
				String line = br.readLine();
				
				// Retirer les 4 premières lignes de comments du .txt d'abord
				while (line !=null){
					// Vérifier le \\t du split de tab
					String[] lineSp = line.toString().split("\\t");
					int nodeId = Integer.parseInt(lineSp[0]);
					int outNodeId = Integer.parseInt(lineSp[1]);
					if (PageRank.map.get(nodeId) != null){
						PageRank.map.get(nodeId).getAdjacentList().add(Node(outNodeId));
					} else {
						PageRank.map.put(nodeId, Node(nodeId));
						PageRank.map.get(nodeId).setPageRank(1);
						PageRank.map.get(nodeId).getAdjacentList().add(Node(outNodeId));
					}
					// go to the next line
					line = br.readLine();
				}
					
				// Emit
				for (int nodeid: PageRank.map.keySet()){
					double p = PageRank.map.get(nodeid).getPageRank()/PageRank.map.get(nodeid).getAdjacentList.length;
					context.write(nodeid, PageRank.map.get(nodeid));
					for (Node m: PageRank.map.get(nodeid).getAdjacentList()){
						context.write(m.nid, p);
					}
				}
			}
			finally{
				//close the file
				in.close();
				fs.close();
			}
	         for (String token: value.toString().split("\\s+")) {
	        	if (hmap.containsKey(token)) {
	        		Integer count = hmap.get(token);
	        		hmap.put(token, count + 1);
	        	} else {
	        		hmap.put(token, 1);
	        	}
	         }
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
         System.out.println(key + " word has count " + sum);
      }
   }

}
