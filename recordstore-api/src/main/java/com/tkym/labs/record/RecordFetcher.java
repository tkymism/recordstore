package com.tkym.labs.record;



public interface RecordFetcher {
	RecordKey getKey() throws RecordstoreException;	
	Record getRecord() throws RecordstoreException;
	boolean next() throws RecordstoreException;
	void close() throws RecordstoreException;
//	<T> T getValue(String columnName, ColumnMetaType<T> columnMetaType) throws RecordstoreException;
}
