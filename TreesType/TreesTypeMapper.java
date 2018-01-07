
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TreesTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
	private final static IntWritable one = new IntWritable(1);


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	String[] attributes = value.toString().split(";");
    	String genre = attributes[2];
    	
    	context.write(new Text(genre), one);
    	}
    }

		public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    while (context.nextKeyValue()) {
		        map(context.getCurrentKey(), context.getCurrentValue(), context);
		    }
		    cleanup(context);
		}

		}

