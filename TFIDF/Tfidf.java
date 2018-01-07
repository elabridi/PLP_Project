package edu.centrale.plp.tfidf;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.lang.*;

import org.apache.commons.math3.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

import com.google.common.collect.Lists;

public class Tfidf extends Configured implements Tool {
	
	private static int totalDocs = 0;

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		try {
			System.out.println("begin run");
			System.out.println(Arrays.toString(arg0));
			Job job = new Job(getConf(), "TFIDFWordCount");
			System.out.println("after job set");
			job.setJarByClass(Tfidf.class);
			job.setOutputKeyClass(LongWritableText.class);
			job.setOutputValueClass(IntWritable.class);

			System.out.println("after output classes");
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			System.out.println("after map reduce classes");
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			job.setMapOutputKeyClass(LongWritableText.class);
		    job.setMapOutputValueClass(IntWritable.class);
			
			System.out.println("after format classes");
			FileInputFormat.addInputPath(job, new Path(arg0[0]));
			FileInputFormat.addInputPath(job, new Path(arg0[1]));
			FileOutputFormat.setOutputPath(job, new Path(arg0[2]));
			
			System.out.println("before waitforcompletion");
			job.waitForCompletion(true);
			
			// Relancer le job avec les Mapper2 et Reducer2 (deuxième partie) en lui passant le résultat du Reducer1
			Job job2 = new Job(getConf(), "TFIDFWordCount");
			System.out.println("after job2 set");
			job2.setJarByClass(Tfidf.class);
			job2.setOutputKeyClass(LongWritableText.class);
			job2.setOutputValueClass(TwoIntWritable.class);

			System.out.println("after2 output classes");
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			
			System.out.println("after2 map reduce classes");
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(TextIntWritable.class);
			
			System.out.println("after2 format classes");
			FileInputFormat.addInputPath(job2, new Path(arg0[2] + "/part-r-00000"));
			FileOutputFormat.setOutputPath(job2, new Path(arg0[3]));
			
			System.out.println("before2 waitforcompletion");
			job2.waitForCompletion(true);
			
			// Relancer le job avec les Mapper3 et Reducer3 (troisième partie) en lui passant le résultat du Reducer2
			Job job3 = new Job(getConf(), "TFIDFWordCount");
			System.out.println("after job3 set");
			job3.setJarByClass(Tfidf.class);
			job3.setOutputKeyClass(LongWritableText.class);
			job3.setOutputValueClass(DoubleWritable.class);

			System.out.println("after3 output classes");
			job3.setMapperClass(Map3.class);
			job3.setReducerClass(Reduce3.class);
			
			System.out.println("after3 map reduce classes");
			job3.setInputFormatClass(SequenceFileInputFormat.class);
			job3.setOutputFormatClass(TextOutputFormat.class);
			
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(LongWritableTwoIntWritable.class);
			
			System.out.println("after3 format classes");
			FileInputFormat.addInputPath(job3, new Path(arg0[3] + "/part-r-00000"));
			FileOutputFormat.setOutputPath(job3, new Path(arg0[4]));
			
			System.out.println("before3 waitforcompletion");
			job3.waitForCompletion(true);
			
		} catch (Exception exception){
			System.out.println("begin catch exception");
			System.out.println(exception.getMessage());
		}
		return 0;
	}

	public static void main(String[] args) {
		try {
			System.out.println("begin main");
			System.out.println(Arrays.toString(args));
		    int res = ToolRunner.run(new Configuration(), new Tfidf(), args);
		    System.out.println("after run before exit");
		    System.exit(res);
		} catch (Exception exception) {
			
		}
	}
	
	public static class Map extends Mapper<LongWritable, Text, LongWritableText, IntWritable> {
	      private final static IntWritable ONE = new IntWritable(1);
	      private Text word = new Text();

	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
				for (String token: value.toString().split("\\s+")) {
					// Emit (word,docid) => 1
					word.set(token);
					LongWritableText docidWord = new Tfidf.LongWritableText(key,word);
					Tfidf.totalDocs += 1;
			        //System.out.println("Emiting (docid, word) => 1 " + key.toString()+ " " + word.toString());
					context.write(docidWord, ONE);
				}
	      }
	      
	}

   public static class Reduce extends Reducer<LongWritableText, IntWritable, LongWritableText, IntWritable> {
      @Override
      public void reduce(LongWritableText key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         // Compte le nombre d'occurences du mot de la key dans le docid de la key
         for (IntWritable val : values) {
            sum += val.get();
         }
         // Ecrit (word,docid) => wordcount
         context.write(key, new IntWritable(sum));
         //System.out.println(key.lw.get() + " " + key.text.toString() + " (docid, word) has count " + sum);
      }
   }
   
   public static class Map2 extends Mapper<LongWritableText, IntWritable, LongWritable, TextIntWritable> {	      

      @Override
      public void map(LongWritableText key, IntWritable value, Context context)
              throws IOException, InterruptedException {
			// Emit docid => (word,wordCount)
			LongWritable docId = key.lw;
			Text word = key.text;
			IntWritable wordCount = value;
	        //System.out.println("Emiting docid => word,wordcount " + docId.toString()+ " " + word.toString() + " " + wordCount.toString());
			context.write(docId, new Tfidf.TextIntWritable(word, wordCount));    
      }
	}

   public static class Reduce2 extends Reducer<LongWritable, TextIntWritable, LongWritableText, TwoIntWritable> {
	   @Override
	   public void reduce(LongWritable key, Iterable<TextIntWritable> values, Context context)
	           throws IOException, InterruptedException {
		  LongWritable docId = key;
		  List<TextIntWritable> valuesList = new ArrayList<TextIntWritable>();
		  for (TextIntWritable val : values) {
				 //System.out.println("begin reduce2 counting wordsPerDoc for the doc id " + key.toString());
		         valuesList.add(new TextIntWritable(new Text(val.text),new IntWritable(val.iw.get())));
		  }
	      int wordsPerDoc = 0;
	      // Compte le nombre de mots wordsperdoc du docid de la key
	      for (TextIntWritable val : valuesList) {
			 //System.out.println("begin reduce2 counting wordsPerDoc for the doc id " + key.toString());
	         wordsPerDoc += val.iw.get();
	      }
		  //System.out.println("wordsPerDoc is " + wordsPerDoc);
	      IntWritable wordsPerDocIW = new IntWritable(wordsPerDoc);
	      for (TextIntWritable val : valuesList) {
			  //System.out.println("begin reduce2 writing word,docid -> wordCount,wordsPerDoc for words of docid " + key.toString());
	    	  Text word = val.text;
	    	  IntWritable wordCount = val.iw;
	    	  // Ecrit (docid,word) => (wordcount,wordsperdoc)
	    	  context.write(new Tfidf.LongWritableText(docId, word), new Tfidf.TwoIntWritable(wordCount, wordsPerDocIW));
	          //System.out.println(docId.toString()+ " " + word.toString() + " (docid, word) has counts " + wordCount.toString() + " " + wordsPerDocIW.toString());
		  }
	   }
   }
   
   public static class Map3 extends Mapper<LongWritableText, TwoIntWritable, Text, LongWritableTwoIntWritable> {	      

	      @Override
	      public void map(LongWritableText key, TwoIntWritable value, Context context)
	              throws IOException, InterruptedException {
	          	//System.out.println("begin map3");
				LongWritable docId = key.lw;
				Text word = key.text;
				IntWritable wordCount = value.iw1;
				IntWritable wordsPerDoc = value.iw2;
				// Emit word => (docId,wordCount, wordsPerDoc)
		        System.out.println("Emiting word => docid,wordcount,wordsperdoc " + word.toString()+ " " + docId.toString() + " " + wordCount.toString() + " " + wordsPerDoc.toString());
				context.write(word, new Tfidf.LongWritableTwoIntWritable(docId, wordCount, wordsPerDoc));    
	      }
	}
   
   private static List<LongWritableTextDoubleWritable> list = new ArrayList<LongWritableTextDoubleWritable>();

   public static class Reduce3 extends Reducer<Text, LongWritableTwoIntWritable, LongWritableText, DoubleWritable> {
	   
	   
	   @Override
	   public void reduce(Text key, Iterable<LongWritableTwoIntWritable> values, Context context)
	           throws IOException, InterruptedException {
	      //System.out.println("begin reduce3");
		  List<LongWritableTwoIntWritable> valuesList = new ArrayList<LongWritableTwoIntWritable>();
	      for (LongWritableTwoIntWritable val : values) {
				 //System.out.println("begin reduce2 counting wordsPerDoc for the doc id " + key.toString());
		         valuesList.add(new LongWritableTwoIntWritable(new LongWritable(val.lw.get()),new IntWritable(val.iw1.get()),new IntWritable(val.iw2.get())));
		  }
	      int docsPerWord = valuesList.size();
          //System.out.println("docsPerWord is " + docsPerWord);
	      IntWritable docsPerWordIW = new IntWritable(docsPerWord);
	      for (LongWritableTwoIntWritable val : valuesList) {
	          //System.out.println("begin longwritabletwointwritable for loop");
	    	  LongWritable docId = val.lw;
	    	  Text word = key;
	    	  IntWritable wordCount = val.iw1;
	          System.out.println(wordCount.get());
	    	  IntWritable wordsPerDoc = val.iw2;
	          System.out.println(wordsPerDoc.get());
	          System.out.println(totalDocs);
	          System.out.println(docsPerWord);
	          //System.out.println(Math.log((double)totalDocs/docsPerWord));
	    	  DoubleWritable tfidf = new DoubleWritable(((double)wordCount.get()/(double)wordsPerDoc.get())*Math.log((double)totalDocs/docsPerWord));
	    	  // Ecrit (word,docid) => tfidf
	    	  context.write(new Tfidf.LongWritableText(docId, word), tfidf);
	    	  list = addInRightOrder(list,new Tfidf.LongWritableTextDoubleWritable(docId, word, tfidf));
	          System.out.println(docId + " " + word + " (docid, word) has tfidf " + tfidf.get());
		  }
          System.out.println("Top 100 tfidf are " + tfidfListFromList(list.subList(0, 100)));
          System.out.println("Top 20 tfidf are " + tfidfListFromList(list.subList(0, 20)));
	   }
   }
   
   private static List<LongWritableTextDoubleWritable> addInRightOrder(List<LongWritableTextDoubleWritable> list,LongWritableTextDoubleWritable element){
	   List<LongWritableTextDoubleWritable> newList = new ArrayList<LongWritableTextDoubleWritable>(list);
	   int listSize = newList.size();
	   if (listSize == 0){
		   newList.add(element);
		   return newList;
	   } else {
		   boolean inserted = false;
		   int i = 0;
		   while (!inserted && i<listSize){
			   LongWritableTextDoubleWritable listIthElement = list.get(i);
			   if (listIthElement.db.compareTo(element.db) <= 0){
				   newList.add(i, element);
				   inserted = true;
			   } else {
				   i += 1;
			   }
		   }
		   if (!inserted){
			   newList.add(listSize-1, element);
		   }
		   return newList;
	   }
   }
   
   private static HashMap<String,Double> tfidfListFromList(List<LongWritableTextDoubleWritable> list){
	   HashMap<String,Double> tfidfList= new HashMap<String,Double>();
	   for (LongWritableTextDoubleWritable lwtdw : list){
		   String docid = lwtdw.iw.toString();
		   String text = lwtdw.text.toString();
		   Double tfidf = lwtdw.db.get();
		   tfidfList.put(docid + " " + text, tfidf);
	   }
	   return tfidfList;
   }
   
   // Méthode pas utilisée, prenait trop de place en mémoire
   private static List<LongWritableTextDoubleWritable> sort(List<LongWritableTextDoubleWritable> unsortedList){
	   double min = 0;
	   List<LongWritableTextDoubleWritable> sortedList = new ArrayList<LongWritableTextDoubleWritable>();
	   List<LongWritableTextDoubleWritable> copy = unsortedList;
	   while (!copy.isEmpty()){
		   LongWritableTextDoubleWritable nextMin = new LongWritableTextDoubleWritable();
		   for (LongWritableTextDoubleWritable wordDocIdTfidf: copy){
			   DoubleWritable tfidf = wordDocIdTfidf.db;
			   if (tfidf.get() < min){
				   min = tfidf.get();
				   nextMin = wordDocIdTfidf;
			   }
		   }
		   copy.remove(nextMin);
		   sortedList.add(nextMin);
	   }
	   return sortedList;
   }
   
   public static class LongWritableText implements WritableComparable<LongWritableText>{
	   
	   private LongWritable lw;
	   
	   private Text text;
	   
	   public LongWritableText(){
		   this.lw = new LongWritable();
		   this.text = new Text();
	   }
	   
	   public LongWritableText(LongWritable lw, Text t){
		   this.lw = lw;
		   this.text = t;
	   }

		@Override
		public void readFields(DataInput in) throws IOException {
			lw.readFields(in);
			text.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			lw.write(out);
			text.write(out);
		}

		@Override
		public int compareTo(LongWritableText o) {
			if (lw.compareTo(o.lw)==0) {
		       return (text.compareTo(o.text));
		     }
		     else return (lw.compareTo(o.lw));
		}
	   
   }
   
   public static class TwoIntWritable implements WritableComparable<TwoIntWritable>{
	   
	   private IntWritable iw1;
	   
	   private IntWritable iw2;
	   
	   public TwoIntWritable(){
		   this.iw1 = new IntWritable();
		   this.iw2 = new IntWritable();
	   }
	   
	   public TwoIntWritable(IntWritable iw1, IntWritable iw2){
		   this.iw1 = iw1;
		   this.iw2 = iw2;
	   }
	   
	   @Override
		public void readFields(DataInput in) throws IOException {
			iw1.readFields(in);
			iw2.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			iw1.write(out);
			iw2.write(out);
		}

		@Override
		public int compareTo(TwoIntWritable o) {
			if (iw1.compareTo(o.iw1)==0) {
		       return (iw2.compareTo(o.iw2));
		     }
		     else return (iw1.compareTo(o.iw1));
		}
	   
   }
   
   public static class TextIntWritable implements WritableComparable<TextIntWritable>{
	   
	   private Text text;
	   
	   private IntWritable iw;
	   
	   public TextIntWritable(){
		   this.text = new Text();
		   this.iw = new IntWritable();
	   }
	   
	   public TextIntWritable(Text text, IntWritable iw){
		   this.text = text;
		   this.iw = iw;
	   }
	   
	   @Override
		public void readFields(DataInput in) throws IOException {
			text.readFields(in);
			iw.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			text.write(out);
			iw.write(out);
		}

		@Override
		public int compareTo(TextIntWritable o) {
			if (text.compareTo(o.text)==0) {
		       return (iw.compareTo(o.iw));
		     }
		     else return (text.compareTo(o.text));
		}
	   
   }
   
   public static class LongWritableTwoIntWritable implements WritableComparable<LongWritableTwoIntWritable>{
	   
	   private LongWritable lw;
	   
	   private IntWritable iw1;
	   
	   private IntWritable iw2;
	   
	   public LongWritableTwoIntWritable(){
		   this.lw = new LongWritable();
		   this.iw1 = new IntWritable();
		   this.iw2 = new IntWritable();
	   }
	   
	   public LongWritableTwoIntWritable(LongWritable lw, IntWritable iw1, IntWritable iw2){
		   this.lw = lw;
		   this.iw1 = iw1;
		   this.iw2 = iw2;
	   }
	   
	   @Override
		public void readFields(DataInput in) throws IOException {
			lw.readFields(in);
			iw1.readFields(in);
			iw2.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			lw.write(out);
			iw1.write(out);
			iw2.write(out);
		}

		@Override
		public int compareTo(LongWritableTwoIntWritable o) {
			if (lw.compareTo(o.lw)==0) {
		       return (new TwoIntWritable(iw1,iw2).compareTo(new TwoIntWritable(o.iw1,o.iw2)));
		     }
		     else return (lw.compareTo(o.lw));
		}
	   
   }

   public static class TripleIntWritable implements WritableComparable<TripleIntWritable>{
	   
	   private IntWritable iw1;
	   
	   private IntWritable iw2;
	   
	   private IntWritable iw3;
	   
	   public TripleIntWritable(){
		   this.iw1 = new IntWritable();
		   this.iw2 = new IntWritable();
		   this.iw3 = new IntWritable();
	   }
	   
	   public TripleIntWritable(IntWritable iw1, IntWritable iw2, IntWritable iw3){
		   this.iw1 = iw1;
		   this.iw2 = iw2;
		   this.iw3 = iw3;
	   }
	   
	   @Override
		public void readFields(DataInput in) throws IOException {
			iw1.readFields(in);
			iw2.readFields(in);
			iw3.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			iw1.write(out);
			iw2.write(out);
			iw3.write(out);
		}

		@Override
		public int compareTo(TripleIntWritable o) {
			if (iw1.compareTo(o.iw1)==0) {
		       return (new TwoIntWritable(iw2,iw3).compareTo(new TwoIntWritable(o.iw2,o.iw3)));
		     }
		     else return (iw1.compareTo(o.iw1));
		}
	   
   }
   
   public static class LongWritableTextDoubleWritable implements WritableComparable<LongWritableTextDoubleWritable>{
	   private LongWritable iw;
	   
	   private Text text;
	   
	   private DoubleWritable db;
	   
	   public LongWritableTextDoubleWritable(){
		   this.iw = new LongWritable();
		   this.text = new Text();
		   this.db = new DoubleWritable();
	   }
	   
	   public LongWritableTextDoubleWritable(LongWritable iw, Text text, DoubleWritable db){
		   this.iw = iw;
		   this.text = text;
		   this.db = db;
	   }
	   
	   @Override
		public void readFields(DataInput in) throws IOException {
			iw.readFields(in);
			text.readFields(in);
			db.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			iw.write(out);
			text.write(out);
			db.write(out);
		}

		@Override
		public int compareTo(LongWritableTextDoubleWritable o) {
			if (iw.compareTo(o.iw)==0) {
				if (text.compareTo(o.text)==0){
					return (db.compareTo(o.db));
				}
				else return (text.compareTo(o.text));
		     }
		     else return (iw.compareTo(o.iw));
		}
   }

}
