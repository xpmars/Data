package com.juanpi.flow;

public class EncrypterFactory {
	
	public static final int seed =  873256;	
	
	public static Encrypter getEncryperInstance(int degree){
		if(degree==0){
			return new DefaultEncrypter();
		}
		if(degree==1){
			return new SecureEncrypter(seed);
		}
		return null;		
	}
	
	
	public static Encrypter getEncryperInstance(String degree){
		if("0".equals(degree)){
			return new DefaultEncrypter();
		}
		if("1".equals(degree)){
			return new SecureEncrypter(seed);
		}
		return new SecureEncrypter(seed);		
	}
}
