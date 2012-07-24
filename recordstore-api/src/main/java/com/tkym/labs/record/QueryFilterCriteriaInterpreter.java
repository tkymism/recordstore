package com.tkym.labs.record;

import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.CONTAIN;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.END_WITH;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.EQUAL;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.GREATER_THAN;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.GREATER_THAN_OR_EQUAL;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.IN;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.LESS_THAN;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.LESS_THAN_OR_EQUAL;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.NOT_EQUAL;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.START_WITH;

import java.util.ArrayList;
import java.util.List;

import com.tkym.labs.record.QueryFilter.QueryFilterOperator;
import com.tkym.labs.record.QueryFilterComposite.QueryFilterCompositeType;




class QueryFilterCriteriaInterpreter{
	List<PsValue> psValue(QueryFilterCriteria criteria){
		if(criteria instanceof QueryFilterComposite)
			return psValue((QueryFilterComposite) criteria);
		else if(criteria instanceof QueryFilter)
			return psValue((QueryFilter<?>) criteria);
		else 
			throw new IllegalArgumentException(
				"interpreter can not resolve type:" + criteria.getClass().getName());
	}
	
	List<PsValue> psValue(QueryFilterComposite composite){
		List<PsValue> ret = new ArrayList<PsValue>();
		for(QueryFilterCriteria criteria : composite.getChildlen())
			ret.addAll(psValue(criteria));
		return ret;
	}
	
	List<PsValue> psValue(QueryFilter<?> filter){
		QueryFilterOperator operator = filter.getOperator();
		List<PsValue> ret = new ArrayList<PsValue>(filter.getValues().length);
		for(Object value : filter.getValues()){
			Object psValue;
			if(value instanceof String)
				psValue = stringAddSuffix((String) value, operator);
			else
				psValue = value;
			ret.add(new PsValue(filter.getProperty(), psValue));
		}
		return ret;
	}
	
	String stringAddSuffix(String str, QueryFilterOperator operator){
		if(operator == START_WITH) return str+"%";
		else if(operator == END_WITH) return "%"+str;
		else if(operator == CONTAIN) return "%"+str+"%";
		else return str;
	}
	
	static class PsValue{
		private final String property;
		private final Object value;
		private PsValue(String property, Object value){
			this.property = property;
			this.value = value;
		}
		String getProperty() {
			return property;
		}
		Object getValue() {
			return value;
		}
	}
	
	String statement(QueryFilterCriteria criteria){
		if(criteria instanceof QueryFilterComposite)
			return statement((QueryFilterComposite) criteria);
		else if(criteria instanceof QueryFilter)
			return statement((QueryFilter<?>) criteria);
		else 
			throw new IllegalArgumentException(
				"interpreter can not resolve type:" + criteria.getClass().getName());
	}
	
	String statement(QueryFilterComposite composite){
		String and_or = "";
		if (composite.getType() == QueryFilterCompositeType.AND) and_or = " and ";
		else if (composite.getType() == QueryFilterCompositeType.OR) and_or = " or ";
		else new IllegalArgumentException("illegal composite type:" + composite.getType());
		
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for(QueryFilterCriteria child : composite.getChildlen()){
			if(first) first = false;
			else sb.append(and_or);
				
			if(child instanceof QueryFilter)
				sb.append(statement((QueryFilter<?>)child));
			else if (child instanceof QueryFilterComposite) 
				sb.append("("+statement((QueryFilterComposite)child)+")");
			else throw new IllegalArgumentException(
					"interpreter can not resolve type:" + child.getClass().getName());
		}
		return sb.toString();
	}
	
	<T> String statement(QueryFilter<T> filter){
		String property = filter.getProperty();
		QueryFilterOperator operator = filter.getOperator();
		if(operator == EQUAL)
			return property + " = ?";
		else if(operator == GREATER_THAN)
			return property + " > ?";
		else if(operator == GREATER_THAN_OR_EQUAL)
			return property + " >= ?";
		else if(operator == LESS_THAN)
			return property + " < ?";
		else if(operator == LESS_THAN_OR_EQUAL)
			return property + " <= ?";
		else if(operator == NOT_EQUAL)
			return property + " <> ?";
		else if(operator == IN)
			return property + " in (" + in(filter.values.length)+")";
		else if( operator == START_WITH || operator == END_WITH || operator == CONTAIN)
			return property + " like ?";
		throw new IllegalArgumentException(
				"property=" + property + 
				", operator=" + operator);
	}
	
	String in(int length){
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for(int i=0; i<length; i++){
			if(first) first = false;
			else sb.append(",");
			sb.append("?");
		}
		return sb.toString();
	}
}