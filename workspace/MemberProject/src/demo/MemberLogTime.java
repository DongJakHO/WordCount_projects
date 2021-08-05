package demo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
//�Զ�������ͣ���������û���������������
public class MemberLogTime implements WritableComparable<MemberLogTime>{
	private String member_name;
	private String logTime;
	public MemberLogTime() {
		
	}//���췽��
	public MemberLogTime(String member_name,String logTime){
		this.member_name=member_name;
		this.logTime=logTime;
	}//���췽��
	public String getMember_name() {//get����
		return member_name;
	}
	public void setMember_name(String member_name) {
		this.member_name = member_name;//set����
	}
	public String getLogTime() {
		return logTime;//get����
	}
	public void setLogTime(String logTime) {
		this.logTime = logTime;//set����
	}
	public void readFields(DataInput in) throws IOException {//��дreadFields
		this.member_name=in.readUTF();//�����ĵ�һ��ֵ��member_name
		this.logTime=in.readUTF();	//�����ĵڶ���ֵ��logTime
	}

	public void write(DataOutput out) throws IOException {//��дwrite
		out.writeUTF(member_name);
		out.writeUTF(logTime);	
	}
	public int compareTo(MemberLogTime o) {//��дcompareTo
		return this.getMember_name().compareTo(o.getMember_name());//�����û�������������
	}
	@Override
	public String toString() {//��дtoString ���
		return this.member_name+","+this.logTime;
	}
}
