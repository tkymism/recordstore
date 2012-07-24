package com.tkym.labs.record;

interface DMLStatementGenerator {
	String asEntity(TableMeta meta);
	String asKey(TableMeta meta);
	String update(TableMeta meta);
	String insert(TableMeta meta);
	String delete(TableMeta meta);
}