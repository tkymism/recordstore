package com.tkym.labs.record;

import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.IN;

public class QueryFilter<T> implements QueryFilterCriteria{
	public enum QueryFilterOperator {
		EQUAL,
		NOT_EQUAL,
		LESS_THAN_OR_EQUAL,
		LESS_THAN,
		GREATER_THAN_OR_EQUAL,
		GREATER_THAN,
		IN,
		START_WITH,
		END_WITH,
		CONTAIN
	}
	private final String property;
	private final QueryFilterOperator operator; 
	final T[] values;
	
	QueryFilter(String property, QueryFilterOperator operator, @SuppressWarnings("unchecked") T... value){
		this.property = property;
		this.operator = operator;
		this.values = value;
		ensureArgument();
	}
	private void ensureArgument(){
		if(values == null)
			throw new NullPointerException("value is null");
		if(values.length == 0)
			throw new IllegalArgumentException("value's length is 0");
		if(operator != IN && values.length != 1)
			throw new IllegalArgumentException("value's length is illegal: length="+values.length);
	}
	public String getProperty() {
		return property;
	}
	public QueryFilterOperator getOperator() {
		return operator;
	}
	public T[] getValues() {
		return values;
	}
	public T getValue(){
		return values[0];
	}
}