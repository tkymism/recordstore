package com.tkym.labs.record;

public interface RecordstoreService {
	public QueryBuilder query(TableMeta meta);
	public Record get(RecordKey key) throws RecordstoreException;
	public void deleteIfExists(RecordKey key) throws RecordstoreException;
	public void delete(RecordKey key) throws RecordstoreException;
	public void update(Record record) throws RecordstoreException;
	public void insert(Record record) throws RecordstoreException;
	public void put(Record record) throws RecordstoreException;
	public RecordstoreTransaction getTransaction();
	public void drop(TableMeta meta, boolean throwException) throws RecordstoreException;
	public void create(TableMeta meta, boolean beforeDrop) throws RecordstoreException;
	public boolean exists(RecordKey key) throws RecordstoreException;
}