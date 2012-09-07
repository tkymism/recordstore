package com.tkym.labs.record;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tkym.labs.record.PreparedStatementProvider.PreparedStatementType;

/**
 */
class PreparedStatementFactory{
	private static Logger LOGGER = LoggerFactory.getLogger(PreparedStatementFactory.class);
	private Connection connection;
	private DMLStatementGenerator helper = new DefaultDmlStatementGenerator();
	
	PreparedStatementFactory(Connection connection){
		this.connection = connection;
	}
	
	/**
	 * 
	 * @param meta
	 * @param type
	 * @param option
	 * @return
	 * @throws SQLException
	 */
	PreparedStatement create(TableMeta meta, PreparedStatementType type, String option)  throws StatementExecuteException{
		if(type.equals(PreparedStatementType.ENTITY)) return asEntity(meta, option);
		if(type.equals(PreparedStatementType.AS_KEY)) return asKey(meta, option);
		if(type.equals(PreparedStatementType.UPDATE)) return update(meta);
		if(type.equals(PreparedStatementType.INSERT)) return insert(meta);
		if(type.equals(PreparedStatementType.DELETE)) return delete(meta);
		throw new IllegalArgumentException(
				"illegalArgument meta is " + meta.toString() + 
				"type is " + type.toString() +
				"option is " + option);
	}
	
	PreparedStatement update(TableMeta meta) throws StatementExecuteException{
		return prepare(helper.update(meta));
	}
	
	PreparedStatement insert(TableMeta meta) throws StatementExecuteException{
		return prepare(helper.insert(meta));
	}
	
	PreparedStatement delete(TableMeta meta) throws StatementExecuteException{
		return prepare(helper.delete(meta));
	}
	
	PreparedStatement asEntity(TableMeta meta, String option) throws StatementExecuteException{
		return prepare(helper.asEntity(meta) + option);
	}
	
	PreparedStatement asKey(TableMeta meta, String option) throws StatementExecuteException{
		return prepare(helper.asKey(meta) + option);
	}
	
	private PreparedStatement prepare(String sql) throws StatementExecuteException{
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(sql); 
			LOGGER.debug("prepare [{}]",sql);
			return preparedStatement;
		} catch (SQLException e) {
			throw new StatementExecuteException(sql, e);
		}
	}
}