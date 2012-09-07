package com.tkym.labs.record;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class SqliteFactory{
	private static final String JDBC_CLASS = "org.sqlite.JDBC"; 
	public SqliteFactory() throws ClassNotFoundException{
		Class.forName(JDBC_CLASS);
	}
	
	public Connection create(URL url) throws SQLException{
		return DriverManager.getConnection("jdbc:sqlite:"+url.getPath());
	}
	
	public static RecordstoreExecutor executor(Connection connection){
		return new RecordstoreExecutor(connection, new SqliteDialect()); 
	} 
	
	public static DDLExecutor ddl(Connection connection){
		return new DefaultDDLExecutor(new StatementExecuteService(connection), new SqliteDialect()); 
	}
}