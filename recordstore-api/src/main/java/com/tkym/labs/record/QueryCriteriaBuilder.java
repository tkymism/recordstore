package com.tkym.labs.record;

import static com.tkym.labs.record.QuerySorter.QuerySortDirection.ASCENDING;
import static com.tkym.labs.record.QuerySorter.QuerySortDirection.DESCENDING;

import com.tkym.labs.record.QueryFilter.QueryFilterOperator;

public class QueryCriteriaBuilder<T> {
	private final String property;
	private final Class<T> type;
	QueryCriteriaBuilder(String property, Class<T> type){
		this.property = property;
		this.type = type;
	}
	QueryCriteriaBuilder(String property){
		this(property, null);
	}
	public QueryFilter<T> equalsTo(T value){
		return create(QueryFilterOperator.EQUAL, value);
	}
	public QueryFilter<T> notEquals(T value){
		return create(QueryFilterOperator.NOT_EQUAL, value);
	}
	public QueryFilter<T> greaterThan(T value){
		return create(QueryFilterOperator.GREATER_THAN, value);
	}
	public QueryFilter<T> greaterEqual(T value){
		return create(QueryFilterOperator.GREATER_THAN_OR_EQUAL, value);
	}
	public QueryFilter<T> lessThan(T value){
		return create(QueryFilterOperator.LESS_THAN, value);
	}
	public QueryFilter<T> lessEqual(T value){
		return create(QueryFilterOperator.LESS_THAN_OR_EQUAL, value);
	}
	public QueryFilter<T> startsWith(T value){
		return create(QueryFilterOperator.START_WITH, value);
	}
	public QueryFilter<T> endsWith(T value){
		return create(QueryFilterOperator.END_WITH, value);
	}
	public QueryFilter<T> contains(T value){
		return create(QueryFilterOperator.CONTAIN, value);
	}
	public QueryFilter<T> in(T... values){
		return new QueryFilter<T>(property, QueryFilterOperator.IN, values);
	}
	@SuppressWarnings("unchecked")
	private QueryFilter<T> create(QueryFilterOperator operator, T value){
		return new QueryFilter<T>(property, operator, value);
	}
	public String getProperty() {
		return property;
	}
	public Class<T> getType() {
		return type;
	}
	public QuerySorter desc(){
		return new QuerySorter(property, DESCENDING);
	}
	public QuerySorter asc(){
		return new QuerySorter(property, ASCENDING);
	}
}