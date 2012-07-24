package com.tkym.labs.record;

import com.tkym.labs.record.QueryFilter.QueryFilterOperator;

public class QueryFilterBuilder<T> {
	private final String propertyName;
	private final QueryBuilder parent;
	QueryFilterBuilder(QueryBuilder parent, String propertyName){
		this.parent = parent;
		this.propertyName = propertyName;
	}
	
	public QueryBuilder equalsTo(T value){
		return filterValue(QueryFilterOperator.EQUAL, value);
	}
	
	public QueryBuilder notEquals(T value){
		return filterValue(QueryFilterOperator.NOT_EQUAL, value);
	}
	
	public QueryBuilder greaterThan(T value){
		return filterValue(QueryFilterOperator.GREATER_THAN, value);
	}
	
	public QueryBuilder greaterEqual(T value){
		return filterValue(QueryFilterOperator.GREATER_THAN_OR_EQUAL, value);
	}
	
	public QueryBuilder lessThan(T value){
		return filterValue(QueryFilterOperator.LESS_THAN, value);
	}
	
	public QueryBuilder lessEqual(T value){
		return filterValue(QueryFilterOperator.LESS_THAN_OR_EQUAL, value);
	}
	
	public QueryBuilder startsWith(T value){
		return filterValue(QueryFilterOperator.START_WITH, value);
	}
	
	public QueryBuilder endsWith(T value){
		return filterValue(QueryFilterOperator.END_WITH, value);
	}
	
	public QueryBuilder contains(T value){
		return filterValue(QueryFilterOperator.CONTAIN, value);
	}
	
	public QueryBuilder in(T... values){
		return parent.filter(new QueryFilter<T>(propertyName, QueryFilterOperator.IN, values));
	}
	
	@SuppressWarnings("unchecked")
	private QueryBuilder filterValue(QueryFilterOperator operator, T value){
		return parent.filter(new QueryFilter<T>(propertyName, operator, value));
	}
}