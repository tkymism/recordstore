package com.tkym.labs.record;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.tkym.labs.record.TableMeta.ColumnMeta;
import com.tkym.labs.record.TableMeta.ColumnMetaType;


class RecordstoreBindHelper{
	private RecordstoreDialect dialect;
	
	RecordstoreBindHelper(RecordstoreDialect dialect){
		this.dialect = dialect;
	}
	
	ResultSetBinder create(ResultSet resultSet, String[] columnNames){
		return dialect.createResultSetBinder(resultSet, columnNames);
	}
	
	PreparedStatementBinder create(PreparedStatement preparedStatement){
		return dialect.createPreparedStatementBinder(preparedStatement);
	}
	
	interface ResultSetBinder {
		public <T> T getValue(String columnName, ColumnMetaType<T> type) throws RecordstoreException;
	}
	
	interface ResultSetGetter{
		String asString() throws RecordstoreException;
		int asInt() throws RecordstoreException;	
		long asLong() throws RecordstoreException;
		short asShort() throws RecordstoreException;
		double asDouble() throws RecordstoreException;
		float asFloat() throws RecordstoreException;
		byte asByte() throws RecordstoreException;
		java.sql.Date asDate() throws RecordstoreException;
		boolean asBoolean() throws RecordstoreException;
		BigDecimal asBigDecimal() throws RecordstoreException;
		Blob asBlob() throws RecordstoreException;
		byte[] asBytes() throws RecordstoreException;
	}
	
	static class ResultSetGetterUsingIndex implements ResultSetGetter{
		private final int columnIndex;
		private final ResultSet resultSet;
		public ResultSetGetterUsingIndex(int columnIndex, ResultSet resultSet) {
			this.columnIndex = columnIndex;
			this.resultSet = resultSet;
		}
		@Override
		public String asString() throws RecordstoreException {
			try {
				return resultSet.getString(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
		@Override
		public int asInt() throws RecordstoreException {
			try {
				return resultSet.getInt(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
		@Override
		public long asLong() throws RecordstoreException {
			try {
				return resultSet.getLong(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
		@Override
		public short asShort() throws RecordstoreException {
			try {
				return resultSet.getShort(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
		@Override
		public double asDouble() throws RecordstoreException {
			try {
				return resultSet.getDouble(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
		@Override
		public float asFloat() throws RecordstoreException {
			try {
				return resultSet.getFloat(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
		@Override
		public byte asByte() throws RecordstoreException {
			try {
				return resultSet.getByte(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
		@Override
		public java.sql.Date asDate() throws RecordstoreException {
			try {
				return resultSet.getDate(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
		@Override
		public boolean asBoolean() throws RecordstoreException {
			try {
				return resultSet.getBoolean(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
		@Override
		public BigDecimal asBigDecimal() throws RecordstoreException {
			try {
				return resultSet.getBigDecimal(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
		@Override
		public Blob asBlob() throws RecordstoreException {
			try {
				return resultSet.getBlob(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
		@Override
		public byte[] asBytes() throws RecordstoreException {
			try {
				return resultSet.getBytes(columnIndex);
			} catch (SQLException e) {
				throw new RecordstoreException("columnIndex "+columnIndex+" occurs SQLException.", e);
			}
		}
	}
	
	static class ResultSetGetterUsingColumnName implements ResultSetGetter{
		private final String columnLabel;
		private final ResultSet resultSet;
		private ResultSetGetterUsingColumnName(String columnLabel, ResultSet resultSet){
			this.columnLabel = columnLabel;
			this.resultSet = resultSet;
		}
		String getColumnLabel(){
			return this.columnLabel;
		}
		public String asString() throws RecordstoreException{
			try {
				return resultSet.getString(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
		public int asInt() throws RecordstoreException{
			try {
				return resultSet.getInt(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
		public long asLong() throws RecordstoreException{
			try {
				return resultSet.getLong(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
		public short asShort() throws RecordstoreException{
			try {
				return resultSet.getShort(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
		public double asDouble() throws RecordstoreException{
			try {
				return resultSet.getDouble(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
		public float asFloat() throws RecordstoreException{
			try {
				return resultSet.getFloat(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
		public byte asByte() throws RecordstoreException{
			try {
				return resultSet.getByte(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
		public java.sql.Date asDate() throws RecordstoreException{
			try {
				return resultSet.getDate(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
		public boolean asBoolean() throws RecordstoreException{
			try {
				return resultSet.getBoolean(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
		public BigDecimal asBigDecimal() throws RecordstoreException{
			try {
				return resultSet.getBigDecimal(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
		public Blob asBlob() throws RecordstoreException{
			try {
				return resultSet.getBlob(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
		public byte[] asBytes() throws RecordstoreException{
			try {
				return resultSet.getBytes(columnLabel);
			} catch (SQLException e) {
				throw new RecordstoreException("columnLabel "+columnLabel+" occurs SQLException.", e);
			}
		}
	}
	
	static class DefaultResultSetBinder implements ResultSetBinder{
		private final ResultSet resultSet;
		private final List<String> columnIndexList;
		DefaultResultSetBinder(ResultSet resultSet, String[] columnNameArray){
			this.resultSet = resultSet;
			this.columnIndexList = Arrays.asList(columnNameArray);
		}
		
		protected ResultSetGetter createResultSetGetter(String columnName, ResultSet resultSet){
			int columnIndex = columnIndexList.indexOf(columnName) + 1;
			return new ResultSetGetterUsingIndex(columnIndex, resultSet);
//			return new ResultSetGetterUsingColumnName(columnName, resultSet);
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public final <T> T getValue(String columnName, ColumnMetaType<T> type) throws RecordstoreException{
			ResultSetGetter getter = createResultSetGetter(columnName, resultSet);
			Object value = null;
			if(type.equals(ColumnMeta.STRING)) 	     value = resolveString(getter);
			else if(type.equals(ColumnMeta.INTEGER)) value = resolveInteger(getter);
			else if(type.equals(ColumnMeta.LONG))	 value = resolveLong(getter);
			else if(type.equals(ColumnMeta.SHORT))	 value = resolveShort(getter);			
			else if(type.equals(ColumnMeta.DOUBLE))	 value = resolveDouble(getter);			
			else if(type.equals(ColumnMeta.FLOAT))	 value = resolveFloat(getter);			
			else if(type.equals(ColumnMeta.BYTE))	 value = resolveByte(getter);			
			else if(type.equals(ColumnMeta.DATE)) 	 value = new Date(resolveDate(getter).getTime());
			else if(type.equals(ColumnMeta.BOOLEAN)) value = resolveBoolean(getter);
			else if(type.equals(ColumnMeta.BYTES))    value = resolveBytes(getter);
			else throw new IllegalArgumentException("unsupport Column Type"+type.getName()+":"+type.getClassType().getName()); 
			return (T) value;
		}
		protected String  resolveString(ResultSetGetter getter)  throws RecordstoreException { return getter.asString(); }
		protected int     resolveInteger(ResultSetGetter getter) throws RecordstoreException { return getter.asInt();    }
		protected long    resolveLong(ResultSetGetter getter)    throws RecordstoreException { return getter.asLong();   }
		protected short   resolveShort(ResultSetGetter getter)   throws RecordstoreException { return getter.asShort();  }
		protected double  resolveDouble(ResultSetGetter getter)  throws RecordstoreException { return getter.asDouble(); }
		protected float   resolveFloat(ResultSetGetter getter)   throws RecordstoreException { return getter.asFloat();  }
		protected byte    resolveByte(ResultSetGetter getter)    throws RecordstoreException { return getter.asByte();   }
		protected Date    resolveDate(ResultSetGetter getter)    throws RecordstoreException { return getter.asDate();   }
		protected boolean resolveBoolean(ResultSetGetter getter) throws RecordstoreException { return getter.asBoolean();}
		protected byte[]  resolveBytes(ResultSetGetter getter)   throws RecordstoreException { return getter.asBytes();  }
	}
	
	interface PreparedStatementBinder{
		<T> void set(Object value, ColumnMetaType<T> type) throws SQLException;
	}
	
	static class DefaultPreparedStatementBinder implements PreparedStatementBinder{
		private PreparedStatement preparedStatement;
		private int index = 1;
		DefaultPreparedStatementBinder(PreparedStatement preparedStatement){
			this.preparedStatement = preparedStatement;
		}
		protected final void pushAs(String value) throws SQLException{
			if(value == null) value = ""; 
			this.preparedStatement.setString(index++, value);
		}
		protected final void pushAs(Integer value) throws SQLException{
			if(value == null) value = new Integer(0); 
			this.preparedStatement.setInt(index++, value);
		}
		protected final void pushAs(Long value) throws SQLException{
			if(value == null) value = new Long(0); 
			this.preparedStatement.setLong(index++, value);
		}
		protected final void pushAs(Short value) throws SQLException{
			if(value == null) value = new Short((short)0);
			this.preparedStatement.setShort(index++, value);
		}
		protected final void pushAs(Double value) throws SQLException{
			if(value == null) value = new Double(0.0d);
			this.preparedStatement.setDouble(index++, value);
		}
		protected final void pushAs(Float value) throws SQLException{
			if(value == null) value = new Float(0.0d);
			this.preparedStatement.setFloat(index++, value);
		}
		protected final void pushAs(Byte value) throws SQLException{
			if(value == null) value = new Byte((byte)0);
			this.preparedStatement.setByte(index++, value);
		}
		protected final void pushAs(Boolean value) throws SQLException{
			if(value == null) value = new Boolean(false);
			this.preparedStatement.setBoolean(index++, value);
		}
		protected final void pushAs(BigDecimal value) throws SQLException{
			if(value == null) value = new BigDecimal(0);
			this.preparedStatement.setBigDecimal(index++, value);
		}
		protected final void pushAs(java.sql.Date value) throws SQLException{
			this.preparedStatement.setDate(index++, value);
		}
		protected final void pushAs(Blob value) throws SQLException{
			this.preparedStatement.setBlob(index++, value);
		}
		protected final void pushAs(byte[] value) throws SQLException{
			this.preparedStatement.setBytes(index++, value);
		}
		@SuppressWarnings("unchecked")
		public final <T> void set(Object value, ColumnMetaType<T> type) throws SQLException{
			try{
				setTypeSafe((T)value, type);
			}catch(ClassCastException e){
				throw new IllegalArgumentException("type is "+type.getName()+", but value is "+value.getClass().getName(),e);
			}
		}
		private <T> void setTypeSafe(T value, ColumnMetaType<T> type) throws SQLException{
			if(type == ColumnMeta.STRING)		resolve((String)value);
			else if(type == ColumnMeta.INTEGER)	resolve((Integer)value);
			else if(type == ColumnMeta.LONG)	resolve((Long)value);
			else if(type == ColumnMeta.SHORT)	resolve((Short)value);
			else if(type == ColumnMeta.DOUBLE)	resolve((Double)value);
			else if(type == ColumnMeta.FLOAT)	resolve((Float)value);
			else if(type == ColumnMeta.BYTE)	resolve((Byte)value);
			else if(type == ColumnMeta.BOOLEAN) resolve((Boolean)value);
			else if(type == ColumnMeta.DATE)	resolve((Date)value);
			else if(type == ColumnMeta.BYTES)    resolve((byte[])value);
			else throw new IllegalArgumentException(
					"unsupport class type:"+value.getClass().getName());
		}
		protected void resolve(String value)  throws SQLException { pushAs(value); }
		protected void resolve(Integer value) throws SQLException { pushAs(value); }
		protected void resolve(Long value)    throws SQLException { pushAs(value); }
		protected void resolve(Short value)   throws SQLException { pushAs(value); }
		protected void resolve(Double value)  throws SQLException { pushAs(value); }
		protected void resolve(Float value)   throws SQLException { pushAs(value); }
		protected void resolve(Byte value)    throws SQLException { pushAs(value); }
		protected void resolve(Boolean value) throws SQLException { pushAs(value); }
		protected void resolve(byte[] value)  throws SQLException { pushAs(value); }
		protected void resolve(Date value)    throws SQLException { 
			pushAs(new java.sql.Date(value.getTime())); 
		}
	}
}
