package com.tkym.labs.record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractQueryResult<T> implements QueryResult<T>{
	protected final RecordFetcher recordFetcher;
	AbstractQueryResult(RecordFetcher recordFetcher){
		this.recordFetcher = recordFetcher;
	}
	
	abstract T createValue() throws RecordstoreException;
	
	@Override
	public T asSingleValue() throws RecordstoreException {
		T result = asIterator().next();
		recordFetcher.close();
		return result;
	}
	
	@Override
	public Iterator<T> asIterator() throws RecordstoreException{
		return new QueryFetchIterator<T>(recordFetcher){
			@Override
			T getValue() throws RecordstoreException{
				return createValue();
			}
		};
	}
	
	@Override
	public List<T> asList() throws RecordstoreException{
		List<T> list = new ArrayList<T>();
		Iterator<T> ite = asIterator();
		while(ite.hasNext())
			list.add(ite.next());
		return list;
	}
}