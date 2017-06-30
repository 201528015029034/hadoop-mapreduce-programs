package MedianStdDev;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianStdDevCombiner extends
	Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable>{
	public void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) 
			throws IOException, InterruptedException {
		SortedMapWritable outValue = new SortedMapWritable();
		for(SortedMapWritable v : values) {
			for(Entry<WritableComparable, Writable> entry : v.entrySet()) {
// ...				
			}
		}
	}
}
