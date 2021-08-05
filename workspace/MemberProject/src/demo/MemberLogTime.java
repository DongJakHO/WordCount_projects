package demo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
//自定义键类型，用来存放用户姓名，访问日期
public class MemberLogTime implements WritableComparable<MemberLogTime>{
	private String member_name;
	private String logTime;
	public MemberLogTime() {
		
	}//构造方法
	public MemberLogTime(String member_name,String logTime){
		this.member_name=member_name;
		this.logTime=logTime;
	}//构造方法
	public String getMember_name() {//get方法
		return member_name;
	}
	public void setMember_name(String member_name) {
		this.member_name = member_name;//set方法
	}
	public String getLogTime() {
		return logTime;//get方法
	}
	public void setLogTime(String logTime) {
		this.logTime = logTime;//set方法
	}
	public void readFields(DataInput in) throws IOException {//重写readFields
		this.member_name=in.readUTF();//读到的第一个值给member_name
		this.logTime=in.readUTF();	//读到的第二个值给logTime
	}

	public void write(DataOutput out) throws IOException {//重写write
		out.writeUTF(member_name);
		out.writeUTF(logTime);	
	}
	public int compareTo(MemberLogTime o) {//重写compareTo
		return this.getMember_name().compareTo(o.getMember_name());//根据用户姓名进行排序
	}
	@Override
	public String toString() {//重写toString 输出
		return this.member_name+","+this.logTime;
	}
}
