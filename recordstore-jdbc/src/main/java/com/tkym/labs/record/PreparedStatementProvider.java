package com.tkym.labs.record;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
	 * 
	 * @param meta
	 * @param type
	 * @param options
	 * @return
	 * @throws PreparedStatementException
	 */
	PreparedStatement get(TableMeta meta, PreparedStatementType type, String... options)  throws PreparedStatementException{
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
	
	/**
	 */
	class PreparedStatementFactory{
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
		PreparedStatement create(TableMeta meta, PreparedStatementType type, String option)  throws PreparedStatementException{
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
		
		PreparedStatement update(TableMeta meta) throws PreparedStatementException{
			return prepare(helper.update(meta));
		}
		
		PreparedStatement insert(TableMeta meta) throws PreparedStatementException{
			return prepare(helper.insert(meta));
		}
		
		PreparedStatement delete(TableMeta meta) throws PreparedStatementException{
			return prepare(helper.delete(meta));
		}
		
		PreparedStatement asEntity(TableMeta meta, String option) throws PreparedStatementException{
			return prepare(helper.asEntity(meta) + option);
		}
		
		PreparedStatement asKey(TableMeta meta, String option) throws PreparedStatementException{
			return prepare(helper.asKey(meta) + option);
		}
		
		private PreparedStatement prepare(String sql) throws PreparedStatementException{
			try {
				return connection.prepareStatement(sql);
			} catch (SQLException e) {
				throw new PreparedStatementException(sql, e);
			}
		}
	}
	
	@SuppressWarnings("serial")
	class PreparedStatementException extends Exception{
		PreparedStatementException(String sql, SQLException e){
			super("Illegal Preparedstatement SQL:["+sql+"]", e);
		}
	}
}