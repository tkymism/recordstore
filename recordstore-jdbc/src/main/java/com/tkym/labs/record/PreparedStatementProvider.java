package com.tkym.labs.record;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 
 */
class PreparedStatementProvider{
	private Map<TableMeta, Map<String, PreparedStatement>> cache = 
			new ConcurrentHashMap<TableMeta, Map<String,PreparedStatement>>();
	private PreparedStatementFactory factory;
	
	/**
	 * @param connection
	 */
	PreparedStatementProvider(Connection connection){
		factory = new PreparedStatementFactory(connection);
	}
	
	/**
	 * @param meta
	 * @param type
	 * @param options
	 * @return
	 * @throws StatementExecuteException
	 */
	PreparedStatement get(TableMeta meta, PreparedStatementType type, String... options)  throws StatementExecuteException{
		Map<String, PreparedStatement> map = getMap(meta);
		String optionStr = optionString(options);
		String key = generateKey(type, optionStr);
		PreparedStatement ps = map.get(key);
		if(ps == null){
			ps = factory.create(meta, type, optionStr);
			map.put(key, ps);
		}
		return ps;
	}
	
	/**
	 * 
	 * @param meta
	 * @return
	 */
	private Map<String, PreparedStatement> getMap(TableMeta meta){
		Map<String, PreparedStatement> map = cache.get(meta);
		if(map == null) {
			map = createMap();
			cache.put(meta, map);
		}
		return map;
	}
	
	/**
	 * 
	 * @param options
	 * @return
	 */
	private String optionString(String... options){
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for(String option : options) {
			if(first) first = false;
			else sb.append(" ");
			sb.append(option);
		}
		return sb.toString();
	}
	
	/**
	 * @param type
	 * @param option
	 * @return
	 */
	private String generateKey(PreparedStatementType type, String option){
		String str = type.toString(); 
		if(option != null) str = str + option;
		return str;
	}
	
	protected Map<String, PreparedStatement> createMap(){
		return new ConcurrentHashMap<String, PreparedStatement>();
	}
	
	/**
	 */
	enum PreparedStatementType{
		UPDATE, INSERT, DELETE, ENTITY, AS_KEY
	}
}