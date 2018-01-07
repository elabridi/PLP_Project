import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TreesTypeHeightMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	String[] attributes = value.toString.split(";");
    	String genre = attributes[2];
	int Height = Integer.parseInt(attributes[6]);
    	
    	context.write(new Text(genre), new IntWritable(Height));
    	}

		public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    while (context.nextKeyValue()) {
		        map(context.getCurrentKey(), context.getCurrentValue(), context);
		    }
		    cleanup(context);
		}

}

