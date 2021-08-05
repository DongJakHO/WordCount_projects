package test;

import java.io.IOException;

public class Submit {

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception,ClassNotFoundException,IOException,InterruptedException {
		// TODO Auto-generated method stub
		String[] arg=new String[]{
				"/user/root/user_login.txt",
				"/user/root/test110",
				","
		};
		new SelectData().main(arg);
	}

}

