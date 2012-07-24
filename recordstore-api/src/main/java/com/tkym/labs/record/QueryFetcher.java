package com.tkym.labs.record;





public interface QueryFetcher<T> {
	public static final QueryFetcher<RecordKey> PRIMARY_KEY = QueryFetcherBuilder.primaryKey();
	public static final QueryFetcher<Record> RECORD = QueryFetcherBuilder.record();
	public boolean isKeyOnly();
	public QueryResult<T> result(RecordFetcher recordFetcher);
}