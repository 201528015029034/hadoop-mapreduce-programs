package MedianStdDevWithNoCombiner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MedianStdDev {
	
	public static class MedianStdDevMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private IntWritable outHour = new IntWritable();
		private IntWritable outCommentLength = new IntWritable();
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
			outHour.set(creationDate.getHours());
			outCommentLength.set(Integer.parseInt(tokens[1]));
			context.write(outHour, outCommentLength);
		}
	}
	
	public static class MedianStdDevReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple> {
		private MedianStdDevTuple result = new MedianStdDevTuple();
		private ArrayList<Float> commentLengths = new ArrayList<Float>();
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
						throws IOException, InterruptedException {
			float sum = 0;
			int count = 0;
			commentLengths.clear();
			result.setStdDev(0);
			for(IntWritable val : values) {
				commentLengths.add((float)val.get());
				sum += val.get();
				count++;
			}
			
			Collections.sort(commentLengths);
			if(count % 2 == 0) {
				result.setMedian((commentLengths.get(count/2 - 1) + commentLengths.get(count/2)) / 2);
			}else {
				result.setMedian(commentLengths.get(count/2));
			}
			float median = sum/count;
			float sumOfSquares = 0.0f;
			for(Float f : commentLengths) {
				sumOfSquares += (f - median) * (f - median);
			}
			result.setStdDev((float)Math.sqrt(sumOfSquares/count));
			context.write(key, result);
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
			job.setMapOutputValueClass(IntWritable.class);
			
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
}
