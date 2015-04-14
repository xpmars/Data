package com.juanpi.flow.exceptions;

public class DataLengthException extends Exception{	
	
	public DataLengthException(String message){
		super(message);
	}
	
	public DataLengthException(Throwable t){
		super(t);
	}

	
	public DataLengthException(String colName, int limit, String value){
		super("The value of " + colName +" exceed length limit "+ limit +" , the value is " + value);
	}
	
}
