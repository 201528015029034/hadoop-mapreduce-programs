package MedianStdDev;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MedianStdDev {
	
	public static class MedianStdDevMapper extends Mapper<LongWritable, Text, IntWritable, SortedMapWritable> {
		private IntWritable commentLength = new IntWritable();
		private static final LongWritable ONE = new LongWritable(1);
		private IntWritable outHour = new IntWritable();
		
		private static final SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\t");
			Date creationDate = null;
			try {
				creationDate = frmt.parse(tokens[0]);
			} catch (ParseException e) {
				e.printStackTrace();
			}
//			Calendar calendar = Calendar.getInstance();
//			calendar.setTime(creationDate);
//outHour.set((int)key.get());
			outHour.set(creationDate.getHours());
			commentLength.set(Integer.parseInt(tokens[1]));
			SortedMapWritable outCommentLength = new SortedMapWritable();
			outCommentLength.put(commentLength, ONE);
System.out.println("hour" + outHour + "outCommentLength" + outCommentLength.size());
			context.write(outHour, outCommentLength);
		}
	}
	
	public static class MedianStdDevReducer extends Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdDevTuple> {
		private MedianStdDevTuple result = new MedianStdDevTuple();
		private TreeMap<Integer, Long> commentLengthCounts = 
				new TreeMap<Integer, Long>();
		
		@Override
		protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
			for(SortedMapWritable smw: values) {
				for(Entry<WritableComparable, Writable> entry: smw.entrySet()) {
System.out.println("realKey: " + key +" key: " + entry.getKey() + " value " + entry.getValue());
				}
System.out.println();
			}
			float sum = 0;
			long totalComments = 0;
			commentLengthCounts.clear();
			result.setMedian(0);
			result.setStdDev(0);
			
			for(SortedMapWritable v: values) {
				for(Entry<WritableComparable, Writable> entry: v.entrySet()) {
					int length = ((IntWritable)entry.getKey()).get();
					long count = ((LongWritable)entry.getValue()).get();
//System.out.println("key: " + key + " length: " + length);
//System.out.println("key: " + key + " count: " + count);
					totalComments += count;
					sum = sum + length*count;
					
					Long storedCount = commentLengthCounts.get(length);
					if(storedCount == null) {
						commentLengthCounts.put(length, count);
					}else {
						commentLengthCounts.put(length, storedCount + count);
					}
				}
			}
			
			//continue 
			long medianIndex = totalComments / 2;
			long previousComments = 0;
			long comments = 0;
			int prevKey = 0;
			for(Entry<Integer, Long> entry: commentLengthCounts.entrySet()) {
				comments = previousComments + entry.getValue();
				if(previousComments <= medianIndex && medianIndex < comments) {
					if(totalComments % 2 == 0 && medianIndex == previousComments) {
						result.setMedian((entry.getKey()+prevKey)/2.0f);
					}else {
						result.setMedian(entry.getKey());
					}
					break;
				}
				previousComments = comments;
				prevKey = entry.getKey();
			}
			
			float mean = sum/totalComments;
			float sumOfSquares = 0.0f;
			for(Entry<Integer, Long> entry: commentLengthCounts.entrySet()) {
				sumOfSquares += (entry.getKey()-mean)*(entry.getKey()-mean)*entry.getValue();
			}
			result.setStdDev((float)Math.sqrt(sumOfSquares/(totalComments-1)));
			context.write(key, result);
		}
	}
	 
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.out.println("args length must be 2");
			System.exit(1);
		}
		
		Job job = Job.getInstance(conf, "medianstddev");
		job.setJarByClass(MedianStdDev.class);
		
		job.setMapperClass(MedianStdDevMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(SortedMapWritable.class);
		
		job.setReducerClass(MedianStdDevReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MedianStdDevTuple.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileSystem fs = FileSystem.get(conf);
		Path outputDir = new Path(otherArgs[1]);
		if(fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
