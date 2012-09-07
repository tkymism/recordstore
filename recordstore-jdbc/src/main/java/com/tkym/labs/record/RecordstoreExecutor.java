package com.tkym.labs.record;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.tkym.labs.record.PreparedStatementProvider.PreparedStatementType;
import com.tkym.labs.record.QueryFilterCriteriaInterpreter.PsValue;
import com.tkym.labs.record.RecordstoreBindHelper.PreparedStatementBinder;
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
	private StatementExecuteService executerService;
	RecordstoreExecutor(Connection connection, RecordstoreDialect dialect) {
		provider = new PreparedStatementProvider(connection);
		executerService = new StatementExecuteService(connection);
		ddlExecutor = new DefaultDDLExecutor(executerService, dialect);
		helper = new RecordstoreBindHelper(dialect);
		this.dialect = dialect;
	}

	public int insert(Record record) throws StatementExecuteException, SQLException{
		RecordKey key = record.key();
		TableMeta meta = record.key().getTableMeta();
		PreparedStatement ps = provider.get(meta, PreparedStatementType.INSERT);
		synchronized (ps) {
			PreparedStatementBinder binder = helper.create(ps);
			bindValuesFromMapAndColumnMeta(binder, meta.keys(), key.asMap());
			bindValuesFromMapAndColumnMeta(binder, meta.properties(), record.asMap());
			return executerService.executeUpdate(ps);
		}
	}
	
	public int update(Record record) throws StatementExecuteException, SQLException{
		RecordKey key = record.key();
		TableMeta meta = record.key().getTableMeta();
		PreparedStatement ps = provider.get(meta, PreparedStatementType.UPDATE);
		synchronized (ps) {
			PreparedStatementBinder binder = helper.create(ps);
			bindValuesFromMapAndColumnMeta(binder, meta.properties(), record.asMap());
			bindValuesFromMapAndColumnMeta(binder, meta.keys(), key.asMap());
			return executerService.executeUpdate(ps);
		}
	}
	
	public int delete(RecordKey key)  throws StatementExecuteException, SQLException {
		TableMeta meta = key.getTableMeta();
		PreparedStatement ps = provider.get(meta, PreparedStatementType.DELETE);
		synchronized (ps) {
			PreparedStatementBinder binder = helper.create(ps);
			bindValuesFromMapAndColumnMeta(binder, meta.keys(), key.asMap());
			return executerService.executeUpdate(ps);
		}
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
			QuerySorterCriteria sorter, boolean isKeyOnly) throws StatementExecuteException, SQLException{
		
		PreparedStatementType type = PreparedStatementType.ENTITY;
		if (isKeyOnly) type = PreparedStatementType.AS_KEY;
		
		PreparedStatement ps = provider.get(meta, type, createQueryStatement(filter, sorter));
		synchronized (ps) {
			PreparedStatementBinder binder = helper.create(ps);
			if (filter != null)
				for (PsValue psValue : filterInterpreter.psValue(filter))
					binder.set(psValue.getValue(), meta.metaTypeOf(psValue.getProperty()));
			return new RecordFetcherImpl(meta, executeNative(ps), dialect);
		}
	}
	
	private ResultSet executeNative(PreparedStatement ps) throws SQLException, StatementExecuteException{
		return executerService.executeQuery(ps);
	}
	
	private String createQueryStatement(QueryFilterCriteria filter,
			QuerySorterCriteria sorter) {
		StringBuilder sb = new StringBuilder();
		if (filter != null)
			sb.append("where " + filterInterpreter.statement(filter)+" ");
		if (sorter != null)
			sb.append("order by " + sorterInterpreter.statement(sorter));
		return sb.toString();
	}
	
	public void drop(TableMeta meta) throws SQLException, StatementExecuteException{
		ddlExecutor.drop(meta);
	}
	
	public void create(TableMeta meta) throws SQLException, StatementExecuteException{
		ddlExecutor.create(meta);
	}
}