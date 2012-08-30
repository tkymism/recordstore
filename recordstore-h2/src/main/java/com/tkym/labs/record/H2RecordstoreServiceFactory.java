package com.tkym.labs.record;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class H2RecordstoreServiceFactory implements RecordstoreServiceFactory{
	static final String JDBC_CLASS = "org.h2.Driver"; 
	private URL url = null;
	
	public H2RecordstoreServiceFactory(URL url) {
		this.url = url;
	}
	
	@Override
	public RecordstoreService create() throws RecordstoreException {
		try {
			Class.forName(JDBC_CLASS);
			Connection connection;
			if (url == null)
				connection = DriverManager.getConnection("jdbc:h2::memory:");
			else
				connection = DriverManager.getConnection("jdbc:h2:"+url.getPath());
			connection.setAutoCommit(false);
			return new RecordstoreServiceImpl(connection, new H2Dialect());
		} catch (ClassNotFoundException e) {
			throw new RecordstoreException(e);
		} catch (SQLException e) {
			throw new RecordstoreException(e);
		}
	}
}