package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 对输入数据从大到小排序
 * @author hadoop
 */
public class DataSort {
	
	public static class DataSortMapper 
		extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		private Text val = new Text("");

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if(line.trim().length() > 0) {
				context.write(new IntWritable(Integer.parseInt(line.trim())), val);
			}
		}
	}
	
	public static class DataSortReducer 
		extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
		
		private IntWritable num = new IntWritable(1);

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
						throws IOException, InterruptedException {
			for(Text val: values) {
				context.write(num, key);
				num = new IntWritable(num.get() + 1);
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 2) {
			System.out.println("args length must be 2");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "datasort");
		job.setJarByClass(DataSort.class);
		
		job.setMapperClass(DataSortMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(DataSortReducer.class);
//		job.setNumReduceTasks(2);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
