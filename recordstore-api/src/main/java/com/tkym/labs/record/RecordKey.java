package com.tkym.labs.record;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.tkym.labs.record.TableMeta.ColumnMeta;




public class RecordKey{
	private final Object[] values;
	private final TableMeta tableMeta;
	public RecordKey(TableMeta meta, Object... values){
		this.tableMeta = meta;
		this.values = values;
		ColumnMeta<?>[] keyMeta = this.tableMeta.keys(); 
		if(values.length != keyMeta.length)
			throw new IllegalArgumentException(
					"illegal values length :"+values.length +", " +
							"length of TableMeta is " + keyMeta.length);
		for(int i=0; i<keyMeta.length; i++){
			if(values[i] == null)
				throw new IllegalArgumentException("values ["+i+"] is null");
			if(!keyMeta[i].getType().getClassType().equals(values[i].getClass()))
				throw new IllegalArgumentException(
						"class type is mismatch "+
						keyMeta[i].getType().getClassType().getName()+
						", "+values[i].getClass().getName());
		}
	}
	@SuppressWarnings("unchecked")
	public <T> T value(String keyName, Class<T> cls){
		return (T) value(keyName);
	}
	public Object value(String keyName){
		for(int i=0; i<tableMeta.keys().length; i++)
			if(keyName.equals(tableMeta.keys()[i].getName()))
				return values[i];
		throw new IllegalArgumentException("keyName:"+keyName+"is not exists on "+tableMeta.tableName());
	}
	Object[] getValues() {
		return values;
	}
	public TableMeta getTableMeta() {
		return tableMeta;
	}
	Map<String, Object> asMap(){
		Map<String, Object> valueMap = new HashMap<String, Object>();
		ColumnMeta<?>[] keyMeta = tableMeta.keys();  
		for(int i=0; i<values.length; i++)
			valueMap.put(keyMeta[i].getName(), values[i]);
		return valueMap;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(values);
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RecordKey other = (RecordKey) obj;
		if (!Arrays.equals(values, other.values))
			return false;
		return true;
	}
	public static class RecordKeyBuilder{
		private final TableMeta tableMeta;
		private final Object[] values;
		private final int suffix;
		public RecordKeyBuilder(TableMeta tableMeta, Object... parentValues){
			this.tableMeta = tableMeta;
			this.values = new Object[parentValues.length+1];
			for(int i=0; i<parentValues.length; i++)
				this.values[i] = parentValues[i];
			this.suffix = parentValues.length;
		}
		public RecordKey build(Object value){
			values[this.suffix] = value;
			return new RecordKey(tableMeta, values);
		}
	}
}