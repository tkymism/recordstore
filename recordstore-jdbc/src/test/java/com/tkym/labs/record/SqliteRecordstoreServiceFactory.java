package com.tkym.labs.record;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.tkym.labs.record.RecordstoreServiceImpl;


public class SqliteRecordstoreServiceFactory implements RecordstoreServiceFactory {
	private static final String JDBC_CLASS = "org.sqlite.JDBC"; 
	private final URL url;
	
	
	public SqliteRecordstoreServiceFactory(URL url){
		this.url = url;
	}
	
	public SqliteRecordstoreServiceFactory(){
		this.url = null;
	}
	
	@Override
	public RecordstoreServiceImpl create() throws RecordstoreException{
		try {
			Class.forName(JDBC_CLASS);
			Connection connection;
			if (url == null)
				connection = DriverManager.getConnection("jdbc:sqlite::memory:");
			else
				connection = DriverManager.getConnection("jdbc:sqlite:"+url.getPath());
			connection.setAutoCommit(false);
			return new RecordstoreServiceImpl(connection, new SqliteDialect());
		} catch (ClassNotFoundException e) {
			throw new RecordstoreException(e);
		} catch (SQLException e) {
			throw new RecordstoreException(e);
		}
	}
}
