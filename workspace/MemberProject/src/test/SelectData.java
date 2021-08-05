package test;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
public class SelectData {
	public static class MyMapper extends Mapper<LongWritable, Text,Text,Text> {
		String splitter;
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)//添加set方法
				throws IOException, InterruptedException {
			splitter=context.getConfiguration().get("splitter");
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,Text,Text>.Context context)
				throws IOException, InterruptedException {
			String[] val=value.toString().split(splitter);//按照参数进行分割
			if(val[1].contains("2016-01") || val[1].contains("2016-02")){
				context.write(new Text(val[0]),new Text(val[1]));
			}
		}

	}
	public static void main(String[] args) throws Exception {
		Configuration conf=Utils.getConfiguration();//集群环境配置
		conf.set("splitter", args[2]);//设置参数
		Job job=Job.getInstance(conf, "selectdata");
		job.setJarByClass(SelectData.class);
		job.setMapperClass(MyMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);//设置输入格式
		job.setOutputFormatClass(SequenceFileOutputFormat.class);//设置输出格式
		job.setNumReduceTasks(0);//设置Reducer任务数为0
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
