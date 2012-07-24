package com.tkym.labs.record;


public interface QueryBuilder {
	public QueryResult<Record> record() throws RecordstoreException;
	public QueryResult<RecordKey> key() throws RecordstoreException;
	public QueryBuilder is(RecordKey key);
	public QueryBuilder is(String propertyName, Object value);
	public <T> QueryFilterBuilder<T> filter(String columnName, Class<T> cls);
	public <T> QueryFilterBuilder<T> filter(String columnName);
	public QuerySortBuilder sort(String columnName);
	public QueryBuilder filter(QueryFilterCriteria... filter);
	public QueryBuilder sort(QuerySorter... sorter);
	public <T> QueryResult<T> read(QueryFetcher<T> fetcher) throws RecordstoreException;
}