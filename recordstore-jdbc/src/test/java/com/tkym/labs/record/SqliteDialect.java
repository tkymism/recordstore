package com.tkym.labs.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tkym.labs.record.RecordstoreDialect.AbstractDatastoreDialect;

import com.tkym.labs.record.TableMeta.ColumnMeta;
import com.tkym.labs.record.TableMeta.ColumnMetaType;


/**
 * Sql String Generator
 * @author kazunari
 */
class SqliteDialect extends AbstractDatastoreDialect{
	private enum SqliteColumnType{
		INTEGER,
		TEXT,
		REAL,
		BLOB,
		NULL
	}
	
	protected Map<ColumnMetaType<?>, SqliteColumnType> typeMap = 
			new HashMap<ColumnMetaType<?>, SqliteColumnType>();
	
	SqliteDialect(){
		typeMap.put(ColumnMeta.INTEGER, SqliteColumnType.INTEGER);
		typeMap.put(ColumnMeta.LONG,    SqliteColumnType.INTEGER);
		typeMap.put(ColumnMeta.SHORT,   SqliteColumnType.INTEGER);
		typeMap.put(ColumnMeta.BYTE, 	SqliteColumnType.INTEGER);
		typeMap.put(ColumnMeta.DOUBLE,  SqliteColumnType.REAL);
		typeMap.put(ColumnMeta.FLOAT,   SqliteColumnType.REAL);
		typeMap.put(ColumnMeta.STRING,  SqliteColumnType.TEXT);
		typeMap.put(ColumnMeta.DATE,    SqliteColumnType.INTEGER);
		typeMap.put(ColumnMeta.BOOLEAN, SqliteColumnType.NULL);
	}
	
	@Override
	public String createCreateStatement(TableMeta p){
		StringBuilder sb = new StringBuilder();
		sb.append("create table ");
		sb.append(p.tableName());
		sb.append(" (");
		boolean first = true;
		
		// 
		Set<String> keySet = new HashSet<String>();
		for(ColumnMeta<?> meta : p.keys()) keySet.add(meta.getName());
		
		// add all key and all column; 
		List<ColumnMeta<?>> columnList = new ArrayList<ColumnMeta<?>>();
		for(ColumnMeta<?> meta : p.keys()) 
			columnList.add(meta);
		for(ColumnMeta<?> meta : p.properties()) 
			columnList.add(meta);
		
		for(ColumnMeta<?> column : columnList){
			if(first) first = false;
			else sb.append(", ");
			
			sb.append(column.getName());
			
			if(column.getType() != null)
				if(typeMap.containsKey(column.getType()))
					sb.append(" "+typeMap.get(column.getType()).toString());
			
			if(keySet.contains(column.getName()))
				sb.append(" primarykey");
		}
		sb.append(")");
		
		return sb.toString();
	}
}