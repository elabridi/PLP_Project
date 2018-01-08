package edu.centrale.plp.pagerank;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.ArrayList;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {
	
	public static HashMap<Integer,Node> map = new HashMap<Integer,Node>();
	
	public static HashMap<Integer,Node> nextMap = new HashMap<Integer,Node>();
	
	private double epsilon = 0.1;
	
	public class Node{
		private double pageRank;
		
		private List<Integer> adjacentList = new ArrayList<Integer>();
		
		private int nid;
		
		public Node(){
			this.nid = 0;
			this.pageRank = 0;
		}
		
		public Node(int nid){
			this.nid = nid;
			this.pageRank = 0;
		}
		
		public Node(Node node){
			this.nid = node.getNid();
			this.pageRank = node.getPageRank();
			this.adjacentList = new ArrayList<Integer>(node.getAdjacentList());
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
		
		public List<Integer> addItemAdjacentList(Integer nid){
			this.adjacentList.add(nid);
		}
		
		public Integer getNid(){
			return this.nid;
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		try {
			// Lance le MapReduce une première fois à partir du .txt en entrée
			// Ce qui donne un premier set de scores pagerank pour chaque personne
			System.out.println(Arrays.toString(arg0));
			Job job = new Job(getConf(), "PageRank");
			job.setJarByClass(PageRank.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(NodeWritable.class);
	
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
	
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(Object.class);
	
			FileInputFormat.addInputPath(job, new Path(arg0[0]));
			FileOutputFormat.setOutputPath(job, new Path("output1"));
	
			job.waitForCompletion(true);
			
			// Relance le MapReduce à partir du résultat du dernier Reduce jusqu'à obtenir convergence des pagerank
			int i = 0;
			while (maxDifferencePageRanks(PageRank.map,PageRank.nextMap) > epsilon) {
				PageRank.map = PageRank.nextMap;
				Job job2 = new Job(getConf(), "PageRank");
				job2.setJarByClass(PageRank.class);
				job2.setOutputKeyClass(IntWritable.class);
				job2.setOutputValueClass(NodeWritable.class);
		
				job2.setMapperClass(Map2.class);
				job2.setReducerClass(Reduce.class);
		
				job2.setInputFormatClass(SequenceFileInputFormat.class);
				job2.setOutputFormatClass(SequenceFileOutputFormat.class);
				
				job2.setMapOutputKeyClass(IntWritable.class);
				job2.setMapOutputValueClass(Object.class);
		
				FileInputFormat.addInputPath(job2, new Path("output" + Integer.toString(i+1)));
				FileOutputFormat.setOutputPath(job2, new Path("output" + Integer.toString(i+2));
		
				job2.waitForCompletion(true);
				i += 1;
			}
			PageRank.map = PageRank.nextMap;
			// Relance le MapReduce une dernière fois pour exporter les scores en TextOutputFormat
			Job job3 = new Job(getConf(), "PageRank");
			job3.setJarByClass(PageRank.class);
			job3.setOutputKeyClass(IntWritable.class);
			job3.setOutputValueClass(NodeWritable.class);
	
			job3.setMapperClass(Map2.class);
			job3.setReducerClass(Reduce.class);
	
			job3.setInputFormatClass(SequenceFileInputFormat.class);
			job3.setOutputFormatClass(TextOutputFormat.class);
			
			job3.setMapOutputKeyClass(IntWritable.class);
			job3.setMapOutputValueClass(Object.class);
	
			FileInputFormat.addInputPath(job3, new Path("output" + Integer.toString(i+1));
			FileOutputFormat.setOutputPath(job3, new Path("output" + Integer.toString(i+2));
	
			job3.waitForCompletion(true);
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
	
	// Premier mapper qui initialise le processus depuis le soc-Epinions1.txt
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Object> {
	      
		  private int i = 0;

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
				
				while (line !=null){
					// Ne prend pas en compte les 4 premières lignes de commentaires
					if (i > 3) {
						String[] lineSp = line.toString().split("\\t");
						int nodeId = Integer.parseInt(lineSp[0]);
						int outNodeId = Integer.parseInt(lineSp[1]);
						if (PageRank.map.get(nodeId) != null){
							PageRank.map.get(nodeId).addItemAdjacentList(outNodeId);
						} else {
							PageRank.map.put(nodeId, Node(nodeId));
							PageRank.map.get(nodeId).setPageRank(1);
							PageRank.map.get(nodeId).addItemAdjacentList(outNodeId);
						}
						// go to the next line
						line = br.readLine();
					}
				}
					
				// Emit
				for (int nodeid: PageRank.map.keySet()){
					double p = PageRank.map.get(nodeid).getPageRank()/(double)PageRank.map.get(nodeid).getAdjacentList.length;
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
	      }
	      
	}
	
	// Seconde classse de mapper qui relance le mapreduce depuis le résultat du reduce précédent,
	// pour s'approcher de la convergence des scores pageranks
	public static class Map2 extends Mapper<IntWritable, NodeWritable, IntWritable, Object> {
	      
	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	  		int nid = key.get();
	    	  		Node node = map.get(nid);
	    	  		
				double pageRank = PageRank.map.get(nid).getPageRank();
				List<Integer> adjList = PageRank.map.get(nid).getAdjacentList();
				int adjListSize = adjList.size();
				double pageRankToEmitToAdjacents = (double) pageRank/(double)adjListSize;
				context.write(nid, NodeWritable(node));
				
				// Emit (nid m, pageRank contribution p from node)
				for (int adjNodeid: adjList){
					context.write(adjNodeid, pageRankToEmitToAdjacents);
				}
				
	      }
	}

   public static class Reduce extends Reducer<IntWritable, Object, IntWritable, NodeWritable> {
      @Override
      public void reduce(Integer key, Iterable<Object> values, Context context)
              throws IOException, InterruptedException {
    	  	 int nid = key;
         Node node = new Node();
         double pageRank = 0;
         for (Object obj : values) {
            if (obj instanceof Node) {
            		node = new Node((Node) obj)
            } else {
            		pageRank += (double) obj
            }
         }
         node.setPageRank(pageRank);
         context.write(IntWritable(nid), NodeWritable(node));
         System.out.println(nid + " Node has pageRank " + pageRank);
      }
   }
   
   public static double maxDifferencePageRanks(HashMap<Integer,Node> map,HashMap<Integer,Node> newMap) {
	   double maxDifference = 0;
	   for (int nodeid: map.keySet()) {
		   double difference = (double) Math.abs(map.get(nodeid).getPageRank() - newMap.get(nodeid).getPageRank());
		   if (difference > maxDifference) {
			   maxDifference = difference;
		   }
	   }
	   return maxDifference;
   }
   
   public static class NodeWritable implements WritableComparable<NodeWritable>{
	   private IntWritable nid;
	   
	   private DoubleWritable pageRank;
	   
	   private List<IntWritable> adjacentList = new ArrayList<IntWritable>();
	   
	   public NodeWritable(){
		   this.nid = new IntWritable();
		   this.pageRank = new DoubleWritable();
		   this.adjacentList = new ArrayList<IntWritable>();;
	   }
	   
	   public NodeWritable(Node node){
		   this.nid = node.getNid();
		   this.pageRank = node.getPageRank();
		   this.adjList = node.getAdjacentList();
	   }
	   
	   @Override
		public void readFields(DataInput in) throws IOException {
		   	nid.readFields(in);
		   	pageRank.readFields(in);
		   	adjList.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			nid.write(out);
			pageRank.write(out);
			adjList.write(out);
		}

		@Override
		public int compareTo(NodeWritable o) {
			if (nid.compareTo(o.nid)==0) {
				else return (pageRank.compareTo(o.pageRank));
		     }
		     else return (nid.compareTo(o.nid));
		}
   }

}
