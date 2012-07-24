package com.tkym.labs.record;

import java.util.Iterator;
import java.util.List;

public interface QueryResult<T> {
	public Iterator<T> asIterator() throws RecordstoreException;
	public List<T> asList() throws RecordstoreException;
	public T asSingleValue() throws RecordstoreException;
}