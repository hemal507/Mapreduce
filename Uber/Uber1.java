import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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

public class Uber1 {
	public static class Ubermap extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] rec = line.split(",");
			String[] days ={"Sun","Mon","Tue","Wed","Thu","Fri","Sat"};
			DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
			Date date = null;
			if(rec[3].equals("trips")){
				System.out.println("Skip the header record");
			}
			else{
				try {
					date = df.parse(rec[1]);
				} catch (ParseException e) {
					
					e.printStackTrace();
				}
				String day = days[date.getDay()];
				context.write(new Text(rec[0]+" "+day), new IntWritable(Integer.parseInt(rec[3])));
			}
		}
	}

	public static class Uberred extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			int sum=0;
			IntWritable out = new IntWritable();
			for (IntWritable trips : value) {
				sum += trips.get();
			}
			out.set(sum);
			context.write(key, out);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Uber1");
		job.setJarByClass(Uber1.class);
		job.setMapperClass(Ubermap.class);
		job.setReducerClass(Uberred.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}
