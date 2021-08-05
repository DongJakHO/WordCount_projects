package JavaAPI;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyToLocal {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS","master:8020");
		FileSystem fs=FileSystem.get(conf);
		Path fromPath=new Path("/user/root/friends/friends.txt");
		Path toPath=new Path("E:/");
		fs.copyToLocalFile(false, fromPath,toPath,true);
		fs.close();
	}

}
