import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Titanic1 {

	public static class Titanicmap extends Mapper<LongWritable, Text, Text, FloatWritable>{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
				String line = value.toString();
				String[] rec = line.split(",");
				if(!(rec[5].isEmpty())){
					context.write(new Text(rec[4]), new FloatWritable(Float.parseFloat(rec[5])));
				}	
		}
	}

	public static class Titanicred extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		public void reduce(Text key, Iterable<FloatWritable> value, Context context)
						throws IOException, InterruptedException {
			float avg_age =0;
			float count = 0;
			float sum = 0;
			FloatWritable out = new FloatWritable();
			for (FloatWritable age : value) {
				count += 1;
				sum += age.get();
			}
			avg_age = sum / count;
			out.set(avg_age);
			context.write(key,out);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Titanic1");
		job.setJarByClass(Titanic1.class);
		job.setMapperClass(Titanicmap.class);
		job.setReducerClass(Titanicred.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
