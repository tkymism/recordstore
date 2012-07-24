package com.tkym.labs.record;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import com.tkym.labs.record.TableMeta.ColumnMeta;
import com.tkym.labs.record.TableMeta.ColumnMetaType;


import org.junit.Test;


public class TableMetaTest {
	@Test
	public void testTableMetaBuildCase001(){
		TableMeta meta = 
				TableMeta.table("person")
				.key("id").type(ColumnMeta.INTEGER)
				.key("email").type(ColumnMeta.STRING)
				.column("profile").type(ColumnMeta.STRING)
				.column("profile2").type(ColumnMeta.STRING)
				.meta();
		assertThat(meta.columns().length, is(2));
		assertThat(meta.keys().length, is(2));
		assertThat(meta.keyNames()[0], is("id"));
		assertThat(meta.keyNames()[1], is("email"));
		assertThat(meta.keys()[0].getName(), is("id"));
		assertThat(meta.keys()[1].getName(), is("email"));
		assertThat(meta.columnNames()[0], is("profile"));
		assertThat(meta.columnNames()[1], is("profile2"));
	}
	
	class BindHelper4Rs {
		private final ResultSet resultSet;
		
		BindHelper4Rs(ResultSet resultSet) {
			this.resultSet = resultSet;
		}
		
		@SuppressWarnings("unchecked")
		<T> T getValue(String columnName, ColumnMetaType<T> type)
				throws SQLException {
			Object value = null;
			if (type.equals(ColumnMeta.STRING))
				value = resultSet.getString(columnName);
			if (type.equals(ColumnMeta.INTEGER))
				value = resultSet.getInt(columnName);
			if (type.equals(ColumnMeta.LONG))
				value = resultSet.getLong(columnName);
			if (type.equals(ColumnMeta.SHORT))
				value = resultSet.getShort(columnName);
			if (type.equals(ColumnMeta.DOUBLE))
				value = resultSet.getDouble(columnName);
			if (type.equals(ColumnMeta.FLOAT))
				value = resultSet.getFloat(columnName);
			if (type.equals(ColumnMeta.BYTE))
				value = resultSet.getByte(columnName);
			if (type.equals(ColumnMeta.DATE))
				value = new Date(resultSet.getDate(columnName).getTime());
			if (value != null)
				return (T) value;
			throw new IllegalArgumentException("unsupport class Type:"
					+ type.getName());
		}
	}
}
