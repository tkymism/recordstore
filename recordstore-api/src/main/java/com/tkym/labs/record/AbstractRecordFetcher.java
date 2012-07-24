package com.tkym.labs.record;

import com.tkym.labs.record.TableMeta.ColumnMeta;
import com.tkym.labs.record.TableMeta.ColumnMetaType;

abstract class AbstractRecordFetcher implements RecordFetcher{
	protected final TableMeta tableMeta;
	AbstractRecordFetcher(TableMeta tableMeta){
		this.tableMeta = tableMeta;
	}
	
	public RecordKey getKey() throws RecordstoreException{
		ColumnMeta<?>[] keyMeta = tableMeta.keys();
		Object[] values = new Object[keyMeta.length]; 
		for(int i=0; i<values.length; i++)
			values[i] = getValue(keyMeta[i].getName(), keyMeta[i].getType());
		return new RecordKey(tableMeta, values);
	}
	
	public Record getRecord() throws RecordstoreException{
		Record record = new Record(getKey());
		ColumnMeta<?>[] valuesMeta = tableMeta.columns();
		for(ColumnMeta<?> m : valuesMeta)
			record.put(m.getName(), getValue(m.getName(), m.getType()));
		return record;
	}
	
	abstract <T> T getValue(String columnName, ColumnMetaType<T> columnMetaType) throws RecordstoreException;
}
