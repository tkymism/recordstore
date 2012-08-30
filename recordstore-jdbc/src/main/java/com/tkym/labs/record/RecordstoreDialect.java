package com.tkym.labs.record;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.tkym.labs.record.RecordstoreBindHelper.PreparedStatementBinder;
import com.tkym.labs.record.RecordstoreBindHelper.ResultSetBinder;


interface RecordstoreDialect {
	String createCreateStatement(TableMeta meta);
	String createDropStatement(TableMeta meta);
	String[] createCreateIndexStatements(TableMeta meta);
	ResultSetBinder createResultSetBinder(ResultSet resultSet, String[] columnsArray);
	PreparedStatementBinder createPreparedStatementBinder(PreparedStatement preparedStatement);
	abstract class AbstractDatastoreDialect implements RecordstoreDialect {
		@Override
		public String createDropStatement(TableMeta p){
			StringBuilder sb = new StringBuilder();
			sb.append("drop table ");
			sb.append(p.tableName());
			return sb.toString();
		}
		@Override
		public String[] createCreateIndexStatements(TableMeta meta){
			return new String[0];
		}
		@Override
		public ResultSetBinder createResultSetBinder(ResultSet resultSet, String[] columnsArray){
			return new RecordstoreBindHelper.DefaultResultSetBinder(resultSet, columnsArray);
		}
		@Override
		public PreparedStatementBinder createPreparedStatementBinder(PreparedStatement preparedStatement){
			return new RecordstoreBindHelper.DefaultPreparedStatementBinder(preparedStatement);
		}
	}
}
