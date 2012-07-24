package com.tkym.labs.record;



public class QueryFetcherBuilder{
	public static <T> QueryFetcher<T> builderFetcher(final boolean isKeyOnly, final RecordFetcherConveter<T> converter){
		return new QueryFetcher<T>(){
			@Override
			public boolean isKeyOnly() {
				return isKeyOnly;
			}

			@Override
			public QueryResult<T> result(RecordFetcher recordFetcher) {
				return new AbstractQueryResult<T>(recordFetcher) {
					@Override
					T createValue() {
						return converter.convert(super.recordFetcher);
					}
				};
			}
		};
	}
	
//	public static <T> QueryFetcher<T> buildByKeyName(final String columnName, final ColumnMetaType<T> columnMetaType){
//		return new QueryFetcher<T>(){
//			@Override
//			public boolean isKeyOnly() {
//				return true;
//			}
//			@Override
//			public QueryResult<T> result(RecordFetcher recordFetcher) {
//				return new AbstractQueryResult<T>(recordFetcher) {
//					@Override
//					T createValue() throws RecordstoreException{
//						return super.recordFetcher.getValue(columnName, columnMetaType);
//					}
//				};
//			}
//		};
//	}
//	
	public static <T> QueryFetcher<T> buildKeyFetcher(final PrimaryKeyConverter<T> converter){
		return new AbstractQueryFetcherForKey<T>() {
			@Override
			T getValueFromPrimaryKey(RecordKey key) {
				return converter.convert(key);
			}
		};
	}
	
	public static <T> QueryFetcher<T> buildDataFetcher(final RecordConverter<T> converter){
		return new AbstractQueryFetcherForRecord<T>() {
			@Override
			T getValueFromRecord(Record record) {
				return converter.convert(record);
			}
		};
	}
	
	static QueryFetcher<Record> record(){
		return new AbstractQueryFetcherForRecord<Record>(){
			@Override
			Record getValueFromRecord(Record record) {
				return record;
			}
		};
	}

	static QueryFetcher<RecordKey> primaryKey(){
		return new AbstractQueryFetcherForKey<RecordKey>(){
			@Override
			RecordKey getValueFromPrimaryKey(RecordKey key) {
				return key;
			}
		};
	}
}