package com.tkym.labs.record;

import static com.tkym.labs.record.QueryFilterComposite.QueryFilterCompositeType.AND;
import static com.tkym.labs.record.QueryFilterComposite.QueryFilterCompositeType.OR;

public class QueryFilterUtils {
	public static QueryFilterComposite and(QueryFilterCriteria... criteria){
		return new QueryFilterComposite(AND,criteria);
	}
	public static QueryFilterComposite or(QueryFilterCriteria... criteria){
		return new QueryFilterComposite(OR,criteria);
	}
	public static <T> QueryFilterFactory<T> property(String propertyName, Class<T> type){
		return new QueryFilterFactory<T>(propertyName, type);
	}
	public static QueryFilterFactory<Object> property(String propertyName){
		return new QueryFilterFactory<Object>(propertyName);
	}
}