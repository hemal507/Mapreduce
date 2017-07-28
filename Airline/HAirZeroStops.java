import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HAirZeroStops {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] rec = line.split(",");
			if (!(rec[1].equals("\'N")) && Integer.parseInt(rec[7]) == 0) {
				context.write(new IntWritable(Integer.parseInt(rec[1])), new Text(rec[7]));
			}
		}
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text>{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] rec = line.split(",");
			context.write(new IntWritable(Integer.parseInt(rec[0])), new Text(rec[7]));
			}
		}
	

	
	public static class Reduce extends Reducer<IntWritable, Text, Text, IntWritable>{
		public void reduce(IntWritable key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			String name = new String();
			IntWritable i = new IntWritable() ;
			for (Text joinedval : value) {
				if(joinedval.equals("0")){
					count += 1;
				}
				else{
					name = joinedval.toString();
				}
				i.set(count);
				context.write(new Text(name),i);
			}
		}
	}		

		public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HAirZeroStops");
		job.setJarByClass(HAirZeroStops.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
				
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, Map.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, Map2.class);
		Path outputPath = new Path(args[2]);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 :1);
	}	
}



	