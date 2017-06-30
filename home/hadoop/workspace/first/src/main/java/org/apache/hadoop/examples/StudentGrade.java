package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 根据各科科目的学生成绩，求学生的各科成绩平均值
 * @author hadoop
 *
 */
public class StudentGrade {
	
	public static class GradeMapper 
		extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String line = value.toString();
			if(line.trim().length() > 0) {
				String[] arr = line.split("\t");
				if(arr.length == 2) {
					context.write(new Text(arr[0]), 
							new IntWritable(Integer.parseInt(arr[1])));
				}
			}
		}
		
	}
	
	public static class GradeReducer 
		extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, 
				Context context) throws IOException, InterruptedException{
			
			int sum = 0;
			int num = 0;
			for(IntWritable val : values) {
				sum += val.get();
				num++;
			}
			context.write(key, new DoubleWritable(sum*1.0/num));
		}
	}
	
	/**
	 * @param args	/home/hadoop/workspace/data/studentgrade/input 
	 * 				/home/hadoop/workspace/data/studentgrade/output
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		if(args.length != 2) {
			System.err.println("args length must be 2");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "studentgrade");
		job.setJarByClass(StudentGrade.class);
		
		job.setMapperClass(GradeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(GradeReducer.class);
//		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem fs = FileSystem.get(conf);
		Path outputDir = new Path(args[1]);
		if(fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
