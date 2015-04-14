package com.juanpi.flow;

public class DefaultEncrypter implements Encrypter{

	@Override
	public String encode(String input) {
		return input;
	}
	@Override
	public String decode(String input) {
		return input;
	}
	

}
