package com.tkym.labs.record;

import java.util.HashMap;
import java.util.Map;

import com.tkym.labs.record.RecordstoreDialect.AbstractDatastoreDialect;
import com.tkym.labs.record.TableMeta.ColumnMeta;
import com.tkym.labs.record.TableMeta.ColumnMetaType;

/**
 * http://www.h2database.com/html/datatypes.html
 */
class H2Dialect extends AbstractDatastoreDialect{
	private enum H2ColumnType{
		INT,
		BOOLEAN,
		TINYINT,
		SMALLINT,
		BIGINT,
		IDENTITY,
		DECIMAL,
		DOUBLE,
		REAL,
		TIME,
		DATE,
		TIMESTAMP,
		BINARY(true),
		OTHER,
		VARCHAR(true),
		VARCHAR_IGNORECASE,
		CHAR(true),
		BLOB,
		CLOB,
		UUID,
		ARRAY;
		final boolean needLength;
		private H2ColumnType(boolean length) {
			this.needLength = length;
		}
		private H2ColumnType() {
			this(false);
		}
		boolean isNeedLength(){
			return this.needLength;
		}
	}
	protected Map<ColumnMetaType<?>, H2Dialect.H2ColumnType> typeMap = 
			new HashMap<ColumnMetaType<?>, H2Dialect.H2ColumnType>();
	H2Dialect(){
		typeMap.put(ColumnMeta.INTEGER, H2ColumnType.INT);
		typeMap.put(ColumnMeta.LONG,    H2ColumnType.BIGINT);
		typeMap.put(ColumnMeta.SHORT,   H2ColumnType.SMALLINT);
		typeMap.put(ColumnMeta.BYTE, 	H2ColumnType.TINYINT);
		typeMap.put(ColumnMeta.DOUBLE,  H2ColumnType.DOUBLE);
		typeMap.put(ColumnMeta.FLOAT,   H2ColumnType.REAL);
		typeMap.put(ColumnMeta.STRING,  H2ColumnType.VARCHAR);
		typeMap.put(ColumnMeta.DATE,    H2ColumnType.DATE);
		typeMap.put(ColumnMeta.BOOLEAN, H2ColumnType.BOOLEAN);
		typeMap.put(ColumnMeta.BYTES, 	H2ColumnType.BLOB);
	}
	@Override
	public String[] createCreateIndexStatements(TableMeta meta) {
		String[] ret = new String[meta.indexes().length+1];
		ret[0] = makeCreatePrimaryKey(meta);
		for (int i=0; i<meta.indexes().length; i++)
			ret[i+1] = makeCreateIndexStatement(
					meta.tableName(), meta.indexes()[i].columns());
		return ret;
	}
	private static String makeCreateIndexStatement(String tableName, ColumnMeta<?>... columns){
		return buildCreateIndexStatement("create index ", tableName, columns);
	}
	private static String makeCreatePrimaryKey(TableMeta tableMeta){
		return buildCreateIndexStatement("create primary key ", 
				tableMeta.tableName(), tableMeta.keys());
	}
	private static String buildCreateIndexStatement(String createIndexStr, String tableName, ColumnMeta<?>... columns){
		StringBuilder sb = new StringBuilder();
		sb.append(createIndexStr);
		sb.append("on ");
		sb.append(tableName+" ");
		sb.append("(");
		boolean first = true;
		for (ColumnMeta<?> col : columns){
			if (first) first = false;
			else sb.append(", ");
			sb.append(col.getName());
		}
		sb.append(")");
		return sb.toString();
	}
	
	@Override
	public String createCreateStatement(TableMeta tableMeta) {
		StringBuilder sb = new StringBuilder();
		sb.append("create table ");
		sb.append(tableMeta.tableName());
		sb.append(" (");
		boolean first = true;
		for(ColumnMeta<?> column : tableMeta.allColumn()){
			if(first) first = false;
			else sb.append(", ");
			sb.append(column.getName());
			sb.append(" ");
			sb.append(makeTypeString(column));
			if (tableMeta.isKey(column))
				sb.append(" not null");
		}
		sb.append(")");
		return sb.toString();
	}
	private <T> String makeTypeString(ColumnMeta<T> meta){
		if(meta.getType() == null)
			throw new IllegalArgumentException(meta.getName()+" type is null");
		if(!typeMap.containsKey(meta.getType()))
			throw new IllegalArgumentException(meta.getName()+" type is not registed");
		H2Dialect.H2ColumnType columnType = typeMap.get(meta.getType());
		if (columnType == H2ColumnType.DECIMAL)
			return H2ColumnType.DECIMAL+"("+meta.getLength()+","+meta.getDecimal()+")";
		else if (columnType.isNeedLength())
			return columnType.name()+"("+meta.getLength()+")";
		else
			return columnType.name();
	}
}