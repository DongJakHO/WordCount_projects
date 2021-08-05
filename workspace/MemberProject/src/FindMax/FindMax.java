package FindMax;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FindMax {
	public static class FindMaxMapper
	extends Mapper<LongWritable,Text,Text,IntWritable>{
		Text course=new Text();
		IntWritable score=new IntWritable();
		@Override
		protected void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] values =value.toString().trim().split(" ");
			course.set(values[0]);
			score.set(Integer.parseInt(values[1]));
			context.write(course,score);
			
		}
	}
	public static class FindMaxReducer
	extends Reducer<Text,IntWritable,Text,IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			int maxScore = -1;
			Text course =new Text();
			for(IntWritable score:values){
				if(score.get()>maxScore){
					maxScore=score.get();
					course=key;
				}		
			}
			
			context.write(course, new IntWritable(maxScore));
		}
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=2){
			System.err.println("FindMax <input> <output>");
			System.exit(-1);
		}
			Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "findmax");
		    job.setJarByClass(FindMax.class);
		    job.setMapperClass(FindMaxMapper.class);
		    job.setReducerClass(FindMaxReducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setNumReduceTasks(1);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileSystem.get(conf).delete(new Path(args[1]),true);
		    FileOutputFormat.setOutputPath(job,new Path(args[1]));
		    System.out.println(job.waitForCompletion(true) ? 0 : 1);
		  }

}
