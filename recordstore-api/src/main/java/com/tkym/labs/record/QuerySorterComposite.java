package com.tkym.labs.record;

import java.util.Arrays;
import java.util.List;

class QuerySorterComposite implements QuerySorterCriteria{
	private final List<QuerySorterCriteria> sorters; 
	QuerySorterComposite(List<QuerySorterCriteria> sorters){
		this.sorters = sorters;
	}
	QuerySorterComposite(QuerySorterCriteria... sorters){
		this(Arrays.asList(sorters));
	}
	public List<QuerySorterCriteria> getSorters() {
		return sorters;
	}
}