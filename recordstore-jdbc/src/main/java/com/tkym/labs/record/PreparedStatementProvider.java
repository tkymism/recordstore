package com.tkym.labs.record;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 
 */
class PreparedStatementProvider{
	private Map<TableMeta, Map<String, UseStatistisCounter<PreparedStatement>>> cache = 
			new ConcurrentHashMap<TableMeta, Map<String,UseStatistisCounter<PreparedStatement>>>();
	private PreparedStatementFactory factory;
	private UseStatisticsManager manager = new UseStatisticsManager();
	/**
	 * @param connection
	 */
	PreparedStatementProvider(Connection connection){
		factory = new PreparedStatementFactory(connection);
	}
	
	PreparedStatement get(TableMeta meta, PreparedStatementType type, String... options)  throws StatementExecuteException{
		Map<String, UseStatistisCounter<PreparedStatement>> map = getMap(meta);
		String optionStr = optionString(options);
		String key = generateKey(type, optionStr);
		
		UseStatistisCounter<PreparedStatement> counter = map.get(key);
		if(counter == null){
			PreparedStatement ps = factory.create(meta, type, optionStr);
			counter = manager.manage(ps);
			map.put(key, counter);
		}
		return counter.use();
	}
	
	private Map<String, UseStatistisCounter<PreparedStatement>> getMap(TableMeta meta){
		Map<String, UseStatistisCounter<PreparedStatement>> map = cache.get(meta);
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
	
	protected Map<String, UseStatistisCounter<PreparedStatement>> createMap(){
		return new ConcurrentHashMap<String, UseStatistisCounter<PreparedStatement>>();
	}
	
	/**
	 */
	enum PreparedStatementType{
		UPDATE, INSERT, DELETE, ENTITY, AS_KEY
	}
}