package com.tkym.labs.record;

import java.util.HashMap;
import java.util.Map;

import com.tkym.labs.record.DefaultDmlStatementGenerator;




class DefaultDmlStatementGenerator2 extends DefaultDmlStatementGenerator{
	enum SqliteDataType{
		NULL,
		INTERGER,
		REAL,
		TEXT,
		BLOB
	}
	
	/**
	 * 
	 * @author kazunari
	 */
	class SqliteDataTypeResolver{
		private Map<Class<?>, SqliteDataType> map = new HashMap<Class<?>, SqliteDataType>();
		
		SqliteDataTypeResolver(){
			toInterger(int.class);
			toInterger(Integer.class);
			toInterger(long.class);
			toInterger(Long.class);
			toInterger(short.class);
			toInterger(Short.class);
			toInterger(byte.class);
			toInterger(Byte.class);
			
			toReal(float.class);
			toReal(Float.class);
			toReal(double.class);
			toReal(Double.class);
			
			toText(String.class);
		}
		
		<T> SqliteDataType resolve(Class<T> cls){
			SqliteDataType type = map.get(cls);
			if(type != null) return type;
			return SqliteDataType.NULL; 
		}
		
		private void toInterger(Class<?> cls){ map.put(cls, SqliteDataType.INTERGER);}
		private void toReal(Class<?> cls){ map.put(cls, SqliteDataType.REAL);}
		private void toText(Class<?> cls){ map.put(cls, SqliteDataType.TEXT);}
		@SuppressWarnings("unused")
		private void toBlob(Class<?> cls){ map.put(cls, SqliteDataType.BLOB);}
		@SuppressWarnings("unused")
		private void toNull(Class<?> cls){ map.put(cls, SqliteDataType.NULL);}
	}
}