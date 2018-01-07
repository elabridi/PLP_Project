import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;

public class TreesTypeHeightReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable TypeHeight = new IntWritable();

    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values,
            final Context context) throws IOException, InterruptedException {

        int max = 0;

        Iterator<IntWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
            sum += iterator.next().get();
        	}
		if (sum > max){
			max = sum;
		}	

        TypeHeight.set(max);

        context.write(key, TypeHeight);
    }
}
