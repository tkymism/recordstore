package com.tkym.labs.record;

import java.util.Arrays;
import java.util.List;

class QuerySorterComposite implements QuerySorterCriteria{
	private final List<QuerySorter> sorters; 
	QuerySorterComposite(List<QuerySorter> sorters){
		this.sorters = sorters;
	}
	QuerySorterComposite(QuerySorter... sorters){
		this(Arrays.asList(sorters));
	}
	public List<QuerySorter> getSorters() {
		return sorters;
	}
}