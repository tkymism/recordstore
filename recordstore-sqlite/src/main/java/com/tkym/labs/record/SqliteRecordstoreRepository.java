package com.tkym.labs.record;

import java.net.URL;

public class SqliteRecordstoreRepository {
	public static SqliteRecordstoreServiceFactory asReal(URL url){
		return new SqliteRecordstoreServiceFactory(url);
	}
	public static SqliteRecordstoreServiceFactory asMemory(){
		return new SqliteRecordstoreServiceFactory();
	}
	public static TableMeta MASTER = 
			TableMeta.table("sqlite_master").
			column("name").asString().
			column("tbl_name").asString().
			column("type").asString().
			column("rootpage").asLong().
			column("sql").asString().
			meta();
}
