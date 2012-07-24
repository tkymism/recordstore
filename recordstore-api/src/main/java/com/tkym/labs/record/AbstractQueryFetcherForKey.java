package com.tkym.labs.record;

public abstract class AbstractQueryFetcherForKey<T> implements QueryFetcher<T>{
	abstract T getValueFromPrimaryKey(RecordKey key);
	@Override
	public boolean isKeyOnly() {
		return true;
	}
	@Override
	public QueryResult<T> result(RecordFetcher recordFetcher) {
		return new AbstractQueryResult<T>(recordFetcher) {
			@Override
			T createValue() throws RecordstoreException{
				return getValueFromPrimaryKey(super.recordFetcher.getKey());
			}
		};
	}
}