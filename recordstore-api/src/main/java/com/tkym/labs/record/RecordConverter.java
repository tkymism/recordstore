package com.tkym.labs.record;

public interface RecordConverter<T> {
	public T convert(Record record);
}