import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Youtube1 {

	public static class Youmap extends Mapper<LongWritable, Text, Text, LongWritable>{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] rec = line.split("\t");
			if(rec.length>5){
				context.write(new Text(rec[3]), new LongWritable(1));
			}
		}
	}
	
	public static class Youred extends Reducer<Text, LongWritable, Text, LongWritable>{
		public void reduce(Text key, Iterable<LongWritable> value,Context context) throws IOException, InterruptedException {
			LongWritable out = new LongWritable();
			long sum = 0;
			for (LongWritable cnt : value) {
				sum += cnt.get();
			}
			out.set(sum);
			context.write(key, out);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Youtube1");
		job.setJarByClass(Youtube1.class);
		job.setMapperClass(Youmap.class);
		job.setReducerClass(Youred.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 :1);
	}
}
