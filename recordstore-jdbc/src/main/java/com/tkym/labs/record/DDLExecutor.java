package com.tkym.labs.record;

import java.sql.SQLException;

/**
 * @author takayama
 */
interface DDLExecutor{
	void create(TableMeta meta) throws SQLException;
	void drop(TableMeta meta) throws SQLException;
}