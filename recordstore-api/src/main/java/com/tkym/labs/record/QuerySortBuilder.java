package com.tkym.labs.record;

import com.tkym.labs.record.QuerySorter.QuerySortDirection;

public class QuerySortBuilder {
	private final String propertyName;
	private final QueryBuilder parent;
	QuerySortBuilder(QueryBuilder parent, String propertyName){
		this.parent = parent;
		this.propertyName = propertyName;
	}
	public QueryBuilder asc() {
		return parent.sort(new QuerySorter(propertyName, QuerySortDirection.ASCENDING));
	}
	public QueryBuilder desc() {
		return parent.sort(new QuerySorter(propertyName, QuerySortDirection.DESCENDING));
	}
}