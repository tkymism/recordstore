package com.tkym.labs.record;

import static com.tkym.labs.record.QueryFilterComposite.QueryFilterCompositeType.AND;
import static com.tkym.labs.record.QueryFilterComposite.QueryFilterCompositeType.OR;

public class QueryUtils {
	public static QueryFilterComposite and(QueryFilterCriteria... criteria){
		return new QueryFilterComposite(AND,criteria);
	}
	public static QueryFilterComposite or(QueryFilterCriteria... criteria){
		return new QueryFilterComposite(OR,criteria);
	}
	public static <T> QueryCriteriaBuilder<T> property(String propertyName, Class<T> type){
		return new QueryCriteriaBuilder<T>(propertyName, type);
	}
	public static QueryCriteriaBuilder<Object> property(String propertyName){
		return new QueryCriteriaBuilder<Object>(propertyName);
	}
}