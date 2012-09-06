package com.tkym.labs.record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.tkym.labs.record.QueryFilterComposite.QueryFilterCompositeType;


abstract class AbstractQueryBuilder implements QueryBuilder {
	private final List<QueryFilterCriteria> filters = new ArrayList<QueryFilterCriteria>();
	private final List<QuerySorterCriteria> sorters = new ArrayList<QuerySorterCriteria>();

	public QueryResult<Record> record() throws RecordstoreException{
		return read(QueryFetcher.RECORD);
	}
	
	public QueryResult<RecordKey> key() throws RecordstoreException{
		return read(QueryFetcher.PRIMARY_KEY);
	}
	
	public <T> QueryResult<T> read(QueryFetcher<T> fetcher) throws RecordstoreException{
		return fetcher.result(execute(fetcher.isKeyOnly(), makeQueryFilterCriteria(), makeQuerySorterCriteria()));
	}
	
	abstract RecordFetcher execute(boolean isKeyOnly, QueryFilterCriteria filter, QuerySorterCriteria sorter) throws RecordstoreException;

	private QuerySorterCriteria makeQuerySorterCriteria(){
		QuerySorterCriteria criteria = null;
		if(sorters.size() == 1)
			criteria = sorters.get(0);
		else if(sorters.size() > 1)
			criteria = new QuerySorterComposite(sorters);
		return criteria;
	}
	
	private QueryFilterCriteria makeQueryFilterCriteria(){
		QueryFilterCriteria criteria = null;
		if(filters.size() == 1)
			criteria = filters.get(0);
		else if(filters.size() > 1)
			criteria = new QueryFilterComposite(QueryFilterCompositeType.AND, filters);
		return criteria;
	}
	
	public QueryBuilder is(String propertyName, Object value){
		return filter(propertyName).equalsTo(value).
				sort(propertyName).asc();
	}
	
	public QueryBuilder is(RecordKey key){
		Map<String, Object> keyValueMap = key.asMap(); 
		for(String property : keyValueMap.keySet()){
			if (keyValueMap.get(property) == null) return this;
			is(property,keyValueMap.get(property));
		}
		return this;
	}
	
	public <T> QueryFilterBuilder<T> filter(String columnName, Class<T> cls){
		return new QueryFilterBuilder<T>(this, columnName); 
	}
	
	public <T> QueryFilterBuilder<T> filter(String columnName){
		return new QueryFilterBuilder<T>(this, columnName); 
	}
	
	public QuerySortBuilder sort(String columnName){
		return new QuerySortBuilder(this, columnName); 
	}
	
	public QueryBuilder filter(QueryFilterCriteria... filter){
		this.filters.addAll(Arrays.asList(filter));
		return this;
	}
	
	public QueryBuilder sort(QuerySorterCriteria... sorter){
		this.sorters.addAll(Arrays.asList(sorter));
		return this;
	}
}
