package com.tkym.labs.record;
class UseStatistisCounter<T> {
	private final T source;
	final UseStatisticsMeta meta;
	UseStatistisCounter(long id, T source){
		this.source = source;
		this.meta = new UseStatisticsMeta(id);
	}
	UseStatisticsMeta getMeta(){
		return meta; 
	}
	T getSource(){
		return source;
	}
	T use(){
		meta.use();
		return this.source;
	}
}