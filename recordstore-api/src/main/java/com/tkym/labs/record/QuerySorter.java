package com.tkym.labs.record;


public class QuerySorter implements QuerySorterCriteria{
	public enum QuerySortDirection{
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