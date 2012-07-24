package com.tkym.labs.record;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.tkym.labs.record.PreparedStatementProvider.PreparedStatementException;
import com.tkym.labs.record.PreparedStatementProvider.PreparedStatementType;
import com.tkym.labs.record.RecordstoreBindHelper.PreparedStatementBinder;

import com.tkym.labs.record.QueryFilterCriteriaInterpreter.PsValue;
import com.tkym.labs.record.TableMeta.ColumnMeta;



/**
 * @author takayama
 */
public class RecordstoreExecutor {
	private RecordstoreBindHelper helper;
	private QueryFilterCriteriaInterpreter filterInterpreter = new QueryFilterCriteriaInterpreter();
	private QuerySorterCriteriaInterpreter sorterInterpreter = new QuerySorterCriteriaInterpreter();
	private final PreparedStatementProvider provider;
	private final RecordstoreDialect dialect;
	private DDLExecutor ddlExecutor;
	RecordstoreExecutor(Connection connection, RecordstoreDialect dialect) {
		provider = new PreparedStatementProvider(connection);
		ddlExecutor = new DefaultDDLExecutor(connection, dialect);
		helper = new RecordstoreBindHelper(dialect);
		this.dialect = dialect;
	}
	
	/**
	 * @param record
	 *            Entity
	 * @return
	 * @throws SQLException
	 */
	public int insert(Record record) throws PreparedStatementException, SQLException{
		RecordKey key = record.key();
		TableMeta meta = record.key().getTableMeta();
		PreparedStatement ps = provider.get(meta, PreparedStatementType.INSERT);
		PreparedStatementBinder binder = helper.create(ps);
		bindValuesFromMapAndColumnMeta(binder, meta.keys(), key.asMap());
		bindValuesFromMapAndColumnMeta(binder, meta.columns(), record.asMap());
		try {
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw e;
		}
	}

	/**
	 * @param record
	 * @return
	 * @throws SQLException
	 */
	public int update(Record record) throws PreparedStatementException, SQLException{
		RecordKey key = record.key();
		TableMeta meta = record.key().getTableMeta();
		PreparedStatement ps = provider.get(meta, PreparedStatementType.UPDATE);
		PreparedStatementBinder binder = helper.create(ps);
		bindValuesFromMapAndColumnMeta(binder, meta.columns(), record.asMap());
		bindValuesFromMapAndColumnMeta(binder, meta.keys(), key.asMap());
		return ps.executeUpdate();
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 * @throws SQLException
	 */
	public int delete(RecordKey key)  throws PreparedStatementException, SQLException {
		TableMeta meta = key.getTableMeta();
		PreparedStatement ps = provider.get(meta, PreparedStatementType.DELETE);
		PreparedStatementBinder binder = helper.create(ps);
		bindValuesFromMapAndColumnMeta(binder, meta.keys(), key.asMap());
		return ps.executeUpdate();
	}
	
	public boolean exists(RecordKey key) throws RecordstoreException {
		RecordKey exists = query(key.getTableMeta()).is(key).read(QueryFetcher.PRIMARY_KEY).asSingleValue(); 
		return exists != null;
	}
	
	private void bindValuesFromMapAndColumnMeta(
			PreparedStatementBinder binder, 
			ColumnMeta<?>[] columnMeta, 
			Map<String, Object> valueMap) throws SQLException{
		for(ColumnMeta<?> meta : columnMeta){
			try {
				binder.set(valueMap.get(meta.getName()), meta.getType());
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("Illegal property is "+meta.getName(), e);
			}
		}
	}
	
	QueryBuilderImpl query(TableMeta tableMeta){
		return new QueryBuilderImpl(this, tableMeta);
	}
	
	RecordFetcherImpl executeQuery(TableMeta meta, QueryFilterCriteria filter,
			QuerySorterCriteria sorter, boolean isKeyOnly) throws PreparedStatementException, SQLException{
		
		PreparedStatementType type = PreparedStatementType.ENTITY;
		if (isKeyOnly) type = PreparedStatementType.AS_KEY;
		
		PreparedStatement ps = provider.get(meta, type,
				createQueryStatement(filter, sorter));
		PreparedStatementBinder binder = helper.create(ps);
		
		if (filter != null)
			for (PsValue psValue : filterInterpreter.psValue(filter))
				binder.set(psValue.getValue(), meta.metaTypeOf(psValue.getProperty()));

		return new RecordFetcherImpl(meta, executeNative(ps), dialect);
	}
	
	private ResultSet executeNative(PreparedStatement ps) throws SQLException{
		return ps.executeQuery();
	}
	
	private String createQueryStatement(QueryFilterCriteria filter,
			QuerySorterCriteria sorter) {
		StringBuilder sb = new StringBuilder();
		if (filter != null)
			sb.append(" where " + filterInterpreter.statement(filter));
		if (sorter != null)
			sb.append(" order by " + sorterInterpreter.statement(sorter));
		return sb.toString();
	}
	
	public void drop(TableMeta meta) throws SQLException{
		ddlExecutor.drop(meta);
	}

	public void create(TableMeta meta) throws SQLException{
		ddlExecutor.create(meta);
	}
}