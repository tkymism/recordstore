package com.tkym.labs.record;

public interface RecordFetcherConveter<T>{
	public T convert(RecordFetcher fetcher);
}