package com.tkym.labs.record;

public abstract class AbstractQueryFetcherForRecord<T> implements QueryFetcher<T>{
	abstract T getValueFromRecord(Record record);
	@Override
	public boolean isKeyOnly() {
		return false;
	}
	@Override
	public QueryResult<T> result(RecordFetcher recordFetcher) {
		return new AbstractQueryResult<T>(recordFetcher) {
			@Override
			T createValue() throws RecordstoreException{
				return getValueFromRecord(super.recordFetcher.getRecord());
			}
		};
	}
}