package com.tkym.labs.record;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author takayama
 */
class DefaultDDLExecutor implements DDLExecutor{
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDDLExecutor.class);
	protected final RecordstoreDialect dialect;
	private final StatementExecuteService executeService;
	DefaultDDLExecutor(StatementExecuteService service, RecordstoreDialect dialect){
		this.dialect = dialect;
		this.executeService = service;
	}
	
	@Override
	public void create(TableMeta tableMeta) throws SQLException, StatementExecuteException{
		execute(dialect.createCreateStatement(tableMeta));
		for (String stmt : dialect.createCreateIndexStatements(tableMeta))
			execute(stmt);
	}
	
	@Override
	public void drop(TableMeta tableMeta)  throws SQLException, StatementExecuteException{
		execute(dialect.createDropStatement(tableMeta));
	}
	
	private void execute(String sql) throws SQLException, StatementExecuteException {
		executeService.execute(sql);
		LOGGER.trace("execute[{}]",sql);
	}
}