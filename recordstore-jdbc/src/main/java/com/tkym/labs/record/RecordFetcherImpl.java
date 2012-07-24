package com.tkym.labs.record;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.tkym.labs.record.RecordstoreBindHelper.ResultSetBinder;
import com.tkym.labs.record.TableMeta.ColumnMetaType;

class RecordFetcherImpl extends AbstractRecordFetcher{
	private final ResultSetBinder binder;
	private final ResultSet resultSet;
	RecordFetcherImpl(TableMeta tableMeta, ResultSet resultSet, RecordstoreDialect dialect){
		super(tableMeta);
		this.resultSet = resultSet;
		this.binder = new RecordstoreBindHelper(dialect).create(resultSet,createSelectsArray());
	}
	
	String[] createSelectsArray(){
		int totalPropertyLength = tableMeta.keyNames().length+tableMeta.columnNames().length;
		String[] selectsArray = new String[totalPropertyLength];
		for (int i=0; i<tableMeta.keyNames().length; i++)
			selectsArray[i] = tableMeta.keyNames()[i];
		for (int i=0; i<tableMeta.columnNames().length; i++)
			selectsArray[i+tableMeta.keyNames().length] = tableMeta.columnNames()[i];
		return selectsArray;
	}
	
	public <T> T getValue(String columnName, ColumnMetaType<T> columnMetaType) throws RecordstoreException{
		return binder.getValue(columnName, columnMetaType);
	}
	
	public boolean next() throws RecordstoreException{
		try {
			return resultSet.next();
		} catch (SQLException e) {
			throw new RecordstoreException(e);
		}
	}
	
	public void close() throws RecordstoreException{
		try {
			resultSet.close();
		} catch (SQLException e) {
			throw new RecordstoreException(e);
		}
	}
}