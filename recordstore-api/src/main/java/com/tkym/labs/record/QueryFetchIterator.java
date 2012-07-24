package com.tkym.labs.record;

import java.util.Iterator;

public abstract class QueryFetchIterator<T> implements Iterator<T>{
	protected final RecordFetcher fetcher;
	protected boolean hasNext = false;
	protected T nextValue = null;
	QueryFetchIterator(RecordFetcher fetcher) throws RecordstoreException{
		this.fetcher = fetcher;
		fetch();
	}
	
	private void fetch() throws RecordstoreException{
		hasNext = fetcher.next();
		if(hasNext)
			nextValue = getValue();
		else{
			nextValue = null;
			fetcher.close();
		}
	}
	
	abstract T getValue() throws RecordstoreException;
	
	@Override
	public T next() {
		T ret = nextValue;
		try {
			fetch();
		} catch (RecordstoreException e) {
			throw new DatastoreFetchException(e);
		}
		return ret;
	}
	
	@Override
	public boolean hasNext() {
		return hasNext;
	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException("method:#remove() is unsupport");
	}
	
	@SuppressWarnings("serial")
	static class DatastoreFetchException extends RuntimeException{
		DatastoreFetchException(Throwable t){
			super(t);
		}
	}
}