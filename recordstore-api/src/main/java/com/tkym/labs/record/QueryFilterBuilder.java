package com.tkym.labs.record;


public class QueryFilterBuilder<T> {
	private final QueryCriteriaBuilder<T> factory;
	private final QueryBuilder parent;
	QueryFilterBuilder(QueryBuilder parent, String propertyName, Class<T> type){
		this.parent = parent;
		this.factory = new QueryCriteriaBuilder<T>(propertyName, type);
	}
	QueryFilterBuilder(QueryBuilder parent, String propertyName){
		this.parent = parent;
		this.factory = new QueryCriteriaBuilder<T>(propertyName);
	}
	public QueryBuilder equalsTo(T value){
		return parent.filter(factory.equalsTo(value));
	}
	public QueryBuilder notEquals(T value){
		return parent.filter(factory.notEquals(value));
	}
	public QueryBuilder greaterThan(T value){
		return parent.filter(factory.greaterThan(value));
	}
	public QueryBuilder greaterEqual(T value){
		return parent.filter(factory.greaterEqual(value));
	}
	public QueryBuilder lessThan(T value){
		return parent.filter(factory.lessThan(value));
	}
	public QueryBuilder lessEqual(T value){
		return parent.filter(factory.lessEqual(value));
	}
	public QueryBuilder startsWith(T value){
		return parent.filter(factory.startsWith(value));
	}
	public QueryBuilder endsWith(T value){
		return parent.filter(factory.endsWith(value));
	}
	public QueryBuilder contains(T value){
		return parent.filter(factory.contains(value));
	}
	public QueryBuilder in(@SuppressWarnings("unchecked") T... values){
		return parent.filter(factory.in(values));
	}
}