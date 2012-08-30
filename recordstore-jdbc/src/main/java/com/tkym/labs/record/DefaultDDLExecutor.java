package com.tkym.labs.record;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 
 * @author kazunari
 */
class DefaultDDLExecutor implements DDLExecutor{
	protected Connection connection;
	protected final RecordstoreDialect dialect;
	
	DefaultDDLExecutor(Connection connection, RecordstoreDialect dialect){
		this.connection = connection;
		this.dialect = dialect;
	}
	
	@Override
	public void create(TableMeta tableMeta) throws SQLException{
		execute(dialect.createCreateStatement(tableMeta));
		for (String stmt : dialect.createCreateIndexStatements(tableMeta))
			execute(stmt);
	}
	
	@Override
	public void drop(TableMeta tableMeta)  throws SQLException{
		execute(dialect.createDropStatement(tableMeta));
	}
	
	private void execute(String sql) throws SQLException {
		try {
			Statement statement = connection.createStatement();
			statement.execute(sql);
		} catch (SQLException e) {
			throw e;
		}
	}
}