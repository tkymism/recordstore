package com.tkym.labs.record;

import java.util.Arrays;
import java.util.List;


public class QueryFilterComposite implements QueryFilterCriteria{
	public enum QueryFilterCompositeType{
		AND, OR
	}
	private final QueryFilterCompositeType type;
	private final List<QueryFilterCriteria> childlen;
	QueryFilterComposite(QueryFilterCompositeType type, List<QueryFilterCriteria> childlen){
		this.type = type;
		this.childlen = childlen;
	}
	QueryFilterComposite(QueryFilterCompositeType type, QueryFilterCriteria... criteria){
		this(type, Arrays.asList(criteria));
	}
	public QueryFilterCompositeType getType() {
		return type;
	}
	public List<QueryFilterCriteria> getChildlen() {
		return childlen;
	}
}