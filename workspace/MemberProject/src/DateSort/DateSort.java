package DateSort;

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



public class DateSort {
	public static class MyMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
		Text date=new Text();
		IntWritable count=new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] values =value.toString().split("\t");
			date.set(values[0]);
			count.set(Integer.parseInt(values[1]));
			context.write(count,date);
		}
		
	}
	public static class MyReducer extends Reducer<IntWritable,Text,Text,IntWritable>{

		@Override
		protected void reduce(IntWritable count, Iterable<Text> date,
				Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			for(Text t:date){
				context.write(t, count);
			}
		}
		
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "datesort");
		    job.setJarByClass(DateSort.class);
		    job.setMapperClass(MyMapper.class);
		    job.setReducerClass(MyReducer.class);
		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job,new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

}
