package com.tkym.labs.record;

import java.util.HashMap;
import java.util.Map;

public class Record{
	private final RecordKey key;
	private final Map<String, Object> values;
	Record (RecordKey key, Map<String, Object> values){
		this.key = key;
		this.values = values;
	}
	public Record(RecordKey key){
		this(key, new HashMap<String, Object>());
	}
	public RecordKey key(){
		return key;
	}
	Map<String, Object> asMap(){
		return values;
	}
	public Object put(String property, Object value){
		return values.put(property, value);
	}
	@SuppressWarnings("unchecked")
	public <T> T get(String property, Class<T> cls){
		return (T) values.get(property);
	}
	public Object get(String property){
		Object value = values.get(property);
		if (value==null)
			throw new IllegalArgumentException(
					"value is null of property["+property+"], " +
							"tableName+"+key.getTableMeta().tableName());
		return value;
	}
}