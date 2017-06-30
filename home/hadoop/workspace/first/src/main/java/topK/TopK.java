package topK;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author hadoop
 * topk : The all data to a reducer are sorted. So we define a new type MyIntWritable, 
 *        making it comparing conversing. 
 */

public class TopK {
	
	public static class TopKMapper extends Mapper<LongWritable, Text, MyIntWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
						throws IOException, InterruptedException {
			String line = value.toString().trim();
			if(line.length() > 0) {
				String[] arr = line.split(",");
				if(arr.length == 4) {
					int payment = Integer.parseInt(arr[2]);
					context.write(new MyIntWritable(payment), new Text(""));
				}
			}
		}
	}
	
	public static class TopKReducer extends Reducer<MyIntWritable, Text, Text, MyIntWritable> {
		private int idx = 0;

		@Override
		protected void reduce(MyIntWritable key, Iterable<Text> values,Context context)
						throws IOException, InterruptedException {
			idx++;
			if(idx <= 5) {
				context.write(new Text(idx + ""), key);
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 2) {
			System.err.println("args length must be 2");
			System.exit(1);
		}
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "topk");
		job.setJarByClass(TopK.class);
		
		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(MyIntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(TopKReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyIntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem fs = FileSystem.get(config);
		Path outputDir = new Path(args[1]);
		if(fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}
