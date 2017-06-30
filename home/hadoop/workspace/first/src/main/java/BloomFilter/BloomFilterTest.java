package BloomFilter;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class BloomFilterTest {
	
	public static class BloomFilterMapper 
		extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private BloomFilter filter = new BloomFilter();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Path bfInputPath = new Path("/home/hadoop/workspace/data/bloomfilter/bloomfilter");
			System.out.println("Reading Bloom filter from: " + bfInputPath);
			DataInputStream strm = new DataInputStream(
					new FileInputStream(bfInputPath.toUri().getPath()));
			filter.readFields(strm);
			strm.close();
		}

		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				if(filter.membershipTest(new Key(word.getBytes()))) {
					context.write(value, NullWritable.get());	
					break;
				}
			}
		}
		
		public static void main(String[] args) throws Exception{
			if(args.length != 2) {
				System.err.println("args length must be 2");
				System.exit(1);
			}
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "bloomfilter");
			job.setJarByClass(BloomFilterTest.class);
			
			job.setMapperClass(BloomFilterMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileSystem fs = FileSystem.get(conf);
			Path outputDir = new Path(args[1]);
			if(fs.exists(outputDir)) {
				fs.delete(outputDir, true);
			}
			FileOutputFormat.setOutputPath(job, outputDir);
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		
	}

}
