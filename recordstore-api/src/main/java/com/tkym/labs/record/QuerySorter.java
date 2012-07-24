package com.tkym.labs.record;


class QuerySorter implements QuerySorterCriteria{
	enum QuerySortDirection{
		ASCENDING,
		DESCENDING
	}
	private final String property;
	private final QuerySorter.QuerySortDirection direction; 
	QuerySorter(String property, QuerySorter.QuerySortDirection direction){
		this.property = property;
		this.direction = direction;
	}
	public String getProperty() {
		return property;
	}
	public QuerySorter.QuerySortDirection getDirection() {
		return direction;
	}
}