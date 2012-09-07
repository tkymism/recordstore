package com.tkym.labs.record;


@SuppressWarnings("serial") 
class StatementExecuteException extends Exception{
	StatementExecuteException(Throwable e){
		super("Illegal PreparedStatement", e);
	}
	StatementExecuteException(String sql, Throwable e){
		super("Illegal Preparedstatement SQL:["+sql+"]", e);
	}
}