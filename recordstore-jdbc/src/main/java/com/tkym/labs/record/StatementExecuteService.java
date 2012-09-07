package com.tkym.labs.record;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StatementExecuteService {
	private static final Logger LOGGER = LoggerFactory.getLogger(StatementExecuteService.class);
	private final Connection connection;
	StatementExecuteService(Connection connection){
		this.connection = connection;
	}
	private ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, StatementExecuteService.class.getSimpleName());
		}
	});
	int executeUpdate(final PreparedStatement preparedStatement) throws StatementExecuteException, SQLException{
		try {
			return executorService.submit(new Callable<Integer>() {
				@Override
				public Integer call() throws Exception {
					long start = 0;
					if (LOGGER.isDebugEnabled()) start = System.nanoTime();
					int ret = preparedStatement.executeUpdate(); 
					if (LOGGER.isDebugEnabled()) 
						LOGGER.debug("executeUpdate:[ret="+ret+"]["+(System.nanoTime()-start)+" nanosec]");
					return ret;
				}
			}).get();
		} catch (InterruptedException e) {
			throw new StatementExecuteException(e);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof SQLException)
				throw (SQLException) e.getCause();
			throw new StatementExecuteException(e);
		}
	}
	ResultSet executeQuery(final PreparedStatement preparedStatement) throws StatementExecuteException, SQLException{
		try {
			return executorService.submit(new Callable<ResultSet>() {
				@Override
				public ResultSet call() throws Exception {
					long start = 0;
					if (LOGGER.isDebugEnabled()) start = System.nanoTime();
					ResultSet ret = preparedStatement.executeQuery(); 
					if (LOGGER.isDebugEnabled()) 
						LOGGER.debug("executeQuery:["+(System.nanoTime()-start)+" nanosec]");
					return ret;
				}
			}).get();
		} catch (InterruptedException e) {
			throw new StatementExecuteException(e);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof SQLException)
				throw (SQLException) e.getCause();
			throw new StatementExecuteException(e);
		}
	}
	void execute(final String sql) throws SQLException, StatementExecuteException{
		try {
			executorService.submit(new Callable<Void>(){
				@Override
				public Void call() throws Exception {
					long start = 0;
					if (LOGGER.isDebugEnabled()) start = System.nanoTime();
					boolean ret = connection.createStatement().execute(sql);
					if (LOGGER.isDebugEnabled()) 
						LOGGER.debug("execute:[ret="+ret+"]["+(System.nanoTime()-start)+" nanosec]");
					return null;
				}
			}).get();
		} catch (InterruptedException e) {
			throw new StatementExecuteException(e);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof SQLException)
				throw (SQLException) e.getCause();
			throw new StatementExecuteException(e);
		}
	}
}
