package com.juanpi.flow;

public class SecureEncrypter implements Encrypter{
	
	private int seed = 0;
	
	public SecureEncrypter(int seed){
		this.seed=seed;	
	}	

	@Override
	public String encode(String input) {
		StringBuilder builder =new  StringBuilder();
		for(int i=0;i<input.length();i++){
			int k =(int)input.charAt(i);
			//int code = new Integer(k^flag);
			builder.append("u" + new Integer(k^seed).toString());
		}
		return builder.substring(1);
	}

	@Override
	public String decode(String input) {
		String[] arr=input.split("u");
		StringBuilder builder= new StringBuilder();
		for(String str:arr){
			int t=Integer.valueOf(str);
			t = t ^ seed;
			builder.append((char)t);
		}		
		return builder.toString();
	}

	
	
}
