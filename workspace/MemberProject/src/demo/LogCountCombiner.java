package demo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

//定义输入输出键值对类型
public class LogCountCombiner extends Reducer<MemberLogTime, IntWritable, MemberLogTime, IntWritable> {
@Override
protected void reduce(MemberLogTime key, Iterable<IntWritable> value,//利用reducer的规约进行值的累加
Reducer<MemberLogTime, IntWritable, MemberLogTime, IntWritable>.Context context)
throws IOException, InterruptedException {
int sum=0;
for (IntWritable val : value) {
sum+=val.get();
}
context.write(key, new IntWritable(sum));
//键还是MemberLogTime key，值就是累加后的结果，最后会将新的键值对输出到reduce端
}
}
