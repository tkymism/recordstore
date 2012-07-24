package com.tkym.labs.record;

import com.tkym.labs.record.QuerySorter.QuerySortDirection;

class QuerySorterCriteriaInterpreter{
	String statement(QuerySorterCriteria criteria){
		if(criteria instanceof QuerySorterComposite)
			return statement((QuerySorterComposite)criteria);
		else if (criteria instanceof QuerySorter)
			return statement((QuerySorter)criteria);
		else 
			throw new IllegalArgumentException(
					"criteria is not supported: type="
							+ criteria.getClass().getName());
	}
	
	String statement(QuerySorterComposite composite){
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for(QuerySorter sorter : composite.getSorters()){
			if(first) first = false;
			else sb.append(", ");
			sb.append(statement(sorter));
		}
		return sb.toString();
	}
	
	String statement(QuerySorter sorter){
		if(sorter.getDirection() == QuerySortDirection.ASCENDING)
			return sorter.getProperty();
		else if (sorter.getDirection() == QuerySortDirection.DESCENDING)
			return sorter.getProperty() + " desc";
		else 
			throw new IllegalArgumentException(
					"direction is not supported: direction="+sorter.getDirection());
	}
}