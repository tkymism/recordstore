package com.tkym.labs.record;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcRecordstoreTransaction implements RecordstoreTransaction{
	private Connection connection;
	JdbcRecordstoreTransaction(Connection connection){
		this.connection = connection;
	}
	
	public void commit() throws RecordstoreException{
		try {
			connection.commit();
		} catch (SQLException e) {
			throw new RecordstoreException(e);
		}
	}

	public void rollback() throws RecordstoreException{
		try {
			connection.rollback();
		} catch (SQLException e) {
			throw new RecordstoreException(e);
		}
	}
	
	public void close() throws RecordstoreException{
		try {
			connection.close();
		} catch (SQLException e) {
			throw new RecordstoreException(e);
		}
	}
}