package com.tkym.labs.record;

import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.IN;

class QueryFilter<T> implements QueryFilterCriteria{
	enum QueryFilterOperator {
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
	
	QueryFilter(String property, QueryFilterOperator operator, T... value){
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
	String getProperty() {
		return property;
	}
	QueryFilterOperator getOperator() {
		return operator;
	}
	T[] getValues() {
		return values;
	}
	T getValue(){
		return values[0];
	}
}