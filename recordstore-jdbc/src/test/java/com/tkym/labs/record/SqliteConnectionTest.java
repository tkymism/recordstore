package com.tkym.labs.record;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class SqliteConnectionTest {
	@Test
	public void testSqliteJDBCInstall() throws Exception{
		new SqliteFactory().create(SqliteConnectionTest.class.getResource("test.db"));
	}
	
	@Test
	public void testSqliteSelect() throws Exception{
		Connection connection = new SqliteFactory().create(SqliteConnectionTest.class.getResource("test.db"));
		try {
			Statement stmt = connection.createStatement();
			stmt.execute("create table " 
					+ "person"+" ("
					+ "personId long primary"
					+ ") "
					);
		} catch (SQLException e) {
		}
		connection.prepareStatement("select * from person");
	}
}