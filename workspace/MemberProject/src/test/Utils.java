package test;

import org.apache.hadoop.conf.Configuration;

public class Utils {
public static Configuration getConfiguration() {
	Configuration conf=new Configuration();
	conf.set("mapreduce.app-submission.cross-platform", "true");//设置狂平台提交mapreduce
	conf.set("fs.defaultFS", "hdfs://master:8020");//指定namenode
	conf.set("mapreduce.framework.name", "yarn");//指定yarn框架
	String resourcenode="master";
	conf.set("yarn.resourcemanager.address", resourcenode+"：8032");//指定resourcemanager
	conf.set("yarn.resourcemanager.scheduler.address", resourcenode+"：8030");//指定资源调度器
	conf.set("mapreduce.jobhistory.address",resourcenode+"：10020");//指定jobhistory
	conf.set("mapreduce.job.jar", "C:\\Users\\36962\\Desktop\\select.jar");//指定jar包
	return conf;
	}

}
