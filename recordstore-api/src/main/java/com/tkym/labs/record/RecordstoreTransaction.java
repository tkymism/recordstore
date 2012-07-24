package com.tkym.labs.record;


public interface RecordstoreTransaction {
	public void commit() throws RecordstoreException;
	public void rollback() throws RecordstoreException;
	public void close() throws RecordstoreException;
}
