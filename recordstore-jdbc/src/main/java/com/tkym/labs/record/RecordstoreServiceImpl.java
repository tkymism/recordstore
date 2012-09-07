package com.tkym.labs.record;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;

class RecordstoreServiceImpl implements RecordstoreService{
	private final RecordstoreExecutor executor;
	private final JdbcRecordstoreTransaction transaction;
	RecordstoreServiceImpl(Connection connection, RecordstoreDialect dialect){
		executor = new RecordstoreExecutor(connection, dialect);
		transaction = new JdbcRecordstoreTransaction(connection);
	}
	
	@Override
	public QueryBuilder query(TableMeta meta){
		return new QueryBuilderImpl(executor, meta);
	}
	
	@Override
	public Record get(RecordKey key) throws RecordstoreException{
		Iterator<Record> ite = 
				query(key.getTableMeta()).
				is(key).
				record().asIterator();
		if(ite.hasNext())
			return ite.next();
		else 
			return null;
	}
	
	@Override
	public void deleteIfExists(RecordKey key) throws RecordstoreException{
		if(exists(key))  delete(key);
	}
	
	@Override
	public void put(Record record) throws RecordstoreException{
		if(exists(record.key())) 
			update(record);
		else
			insert(record);
	}
	
	@Override
	public void delete(RecordKey key) throws RecordstoreException{
		try {
			executor.delete(key);
		} catch (Exception e) {
			throw new RecordstoreException(e);
		}
	}
	
	@Override
	public void update(Record record) throws RecordstoreException{
		try {
			executor.update(record);
		} catch (Exception e) {
			throw new RecordstoreException(e);
		}
	}
	
	@Override
	public void insert(Record record) throws RecordstoreException{
		try {
			executor.insert(record);
		} catch (Exception e) {
			throw new RecordstoreException(e);
		}
	}
	
	@Override
	public boolean exists(RecordKey key) throws RecordstoreException{
		return executor.exists(key);
	}
	
	@Override
	public JdbcRecordstoreTransaction getTransaction(){
		return transaction;
	}

	@Override
	public void drop(TableMeta meta, boolean throwException) throws RecordstoreException{
		try {
			executor.drop(meta);
		} catch (SQLException e) {
			if(throwException)
				throw new RecordstoreException(e);
		} catch (StatementExecuteException e) {
			throw new RecordstoreException(e);
		}
	}
	
	@Override
	public void create(TableMeta meta, boolean beforeDrop) throws RecordstoreException{
		try {
			if(beforeDrop) drop(meta, false);
			executor.create(meta);
		} catch (SQLException e) {
			throw new RecordstoreException(e);
		} catch (StatementExecuteException e) {
			throw new RecordstoreException(e);
		}
	}
}