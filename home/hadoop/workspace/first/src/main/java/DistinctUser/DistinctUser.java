package DistinctUser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistinctUser {
	
	public static class DistinctUserMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private Text outUserId = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] items = line.split("\t");
Text last = outUserId;
			outUserId.set(items[0]);
System.out.println(last == outUserId);
			context.write(outUserId, NullWritable.get());
		}		
	}
	
	public static class DistinctUserReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.err.println("args length must be 2.");
			System.exit(1);
		}
		
		Job job = Job.getInstance(conf, "distinctUser");
		job.setJarByClass(DistinctUser.class);
		
		job.setMapperClass(DistinctUserMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setReducerClass(DistinctUserReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		Path outputDir = new Path(otherArgs[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
