package test;

import org.apache.hadoop.conf.Configuration;

public class Utils {
public static Configuration getConfiguration() {
	Configuration conf=new Configuration();
	conf.set("mapreduce.app-submission.cross-platform", "true");//���ÿ�ƽ̨�ύmapreduce
	conf.set("fs.defaultFS", "hdfs://master:8020");//ָ��namenode
	conf.set("mapreduce.framework.name", "yarn");//ָ��yarn���
	String resourcenode="master";
	conf.set("yarn.resourcemanager.address", resourcenode+"��8032");//ָ��resourcemanager
	conf.set("yarn.resourcemanager.scheduler.address", resourcenode+"��8030");//ָ����Դ������
	conf.set("mapreduce.jobhistory.address",resourcenode+"��10020");//ָ��jobhistory
	conf.set("mapreduce.job.jar", "C:\\Users\\36962\\Desktop\\select.jar");//ָ��jar��
	return conf;
	}

}
