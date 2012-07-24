package com.tkym.labs.record;

public interface RecordstoreServiceFactory {
	public RecordstoreService create() throws RecordstoreException;
}