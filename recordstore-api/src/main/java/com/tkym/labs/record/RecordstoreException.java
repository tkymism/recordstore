package com.tkym.labs.record;

@SuppressWarnings("serial")
public class RecordstoreException extends Exception{
	RecordstoreException(Throwable t){
		super(t);
	}
	RecordstoreException(String message, Throwable t){
		super(message, t);
	}
}