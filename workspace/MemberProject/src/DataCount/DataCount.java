package DataCount;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class DataCount {
	public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		Text date=new Text();
		IntWritable one = new IntWritable(1);
		
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] values =value.toString().split(",");
			date.set(values[1]);
			context.write(date, one);
		}	
	}
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		IntWritable count=new IntWritable();
		@Override
		protected void reduce(Text date, Iterable<IntWritable> ones,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum=0;
			for (IntWritable one:ones){
				sum+=one.get();
			}
			count.set(sum);
			context.write(date, count);
		}
		
	}
	public static void main(String[] args) throws Exception  {
		// TODO Auto-generated method stub
		 Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "data count");
		    job.setJarByClass(DataCount.class);
		    job.setMapperClass(MyMapper.class);
		    job.setReducerClass(MyReducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job,new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

	}

