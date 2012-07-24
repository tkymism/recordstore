package com.tkym.labs.record;

public interface PrimaryKeyConverter<T> {
	public T convert(RecordKey key);
}