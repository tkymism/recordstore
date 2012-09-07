package com.tkym.labs.record;

import java.sql.SQLException;


public class QueryBuilderImpl extends AbstractQueryBuilder{
	private final RecordstoreExecutor datastore;
	private final TableMeta tableMeta;
	
	QueryBuilderImpl(RecordstoreExecutor datastore, TableMeta tableMeta){
		this.datastore = datastore;
		this.tableMeta = tableMeta;
	}
	
	RecordFetcherImpl execute(boolean isKeyOnly, QueryFilterCriteria filter, QuerySorterCriteria sorter) throws RecordstoreException{
		try {
			return datastore.executeQuery(tableMeta, 
					filter, 
					sorter, 
					isKeyOnly);
		} catch (SQLException e) {
			throw new RecordstoreException(e);
		} catch (StatementExecuteException e) {
			throw new RecordstoreException(e);
		}
	}
}