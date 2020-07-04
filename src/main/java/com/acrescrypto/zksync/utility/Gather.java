package com.acrescrypto.zksync.utility;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.utility.Util.OpportunisticExceptionHandler;

public class Gather<A,R> {
	public interface GatherVoidFunction<A,R> {
		void invoke(Gather<A,R> gather, A argument) throws Exception;
	}
	
	public interface GatherFunction<A,R> {
		R invoke(Gather<A,R> gather, A argument) throws Exception;
	}
	
	public interface GatherFunctionAsync<A,R> {
		void invoke(Gather<A,R> gather, A argument, ResultFunction<A,R> result) throws Exception;
	}
	
	public interface GatherFinishedFind<A,R> {
		void finished(Gather<A,R> gather, R result) throws Exception;
	}
	
	public interface GatherFinishedMap<A,R> {
		void finished(Gather<A,R> gather, Map<A,R> results) throws Exception;
	}
	
	public interface CheckFunction<A,R> {
		void check(Gather<A,R> gather) throws Exception;
	}
	
	public interface WithFunction<A,R> {
		void with(Gather<A,R> gather, ReadyFunction<A,R> ready) throws Exception;
	}
	
	public interface ResultFunction<A,R> {
		void yield(R result);
	}
	
	public interface ReadyFunction<A,R> {
		void found(Collection<A> arguments);
	}
	
	public interface ExceptionHandler<A,R> {
		void exception(Gather<A,R> gather, Exception exc);
	}
	
	protected Queue             <A  > arguments = new LinkedList<>();
	protected                      R  value;
	protected GatherFunction    <A,R> function;
	protected GatherFinishedFind<A,R> finished;
	protected Map               <A,R> results   = new ConcurrentHashMap<>();
	protected boolean                 stopped,
	                                  withComplete;
	protected int                     numPending;
	protected ExceptionHandler  <A,R> exceptionHandler;

	protected List<CheckFunction<A,R>>           checkFunctions = new LinkedList<>();
	protected Map <WithFunction <A,R>, Boolean>  withFunctions  = new ConcurrentHashMap<>();
	
	protected Logger                  logger = LoggerFactory.getLogger(Gather.class);

	public Gather() {
		onException(defaultExceptionHandler());
	}
	
	public ExceptionHandler<A,R> defaultExceptionHandler() {
		return (g, xx) -> {
			try {
				throw(xx);
			} catch(SecurityException exc) {
				// TODO: add ability to close and/or blacklist a connection
				// also want ProtocolViolationError and so on in here too
				logger.error("Gather caught security exception without registered socket");
			} catch(Exception exc) {
				logger.error("Gather caught exception without registered exception handler", exc);
			}
		};
	}
	
	public Gather<A,R> onException(ExceptionHandler<A,R> exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
		return this;
	}
	
	public Gather<A,R> exception(Exception exc) {
		stopped = true;
		this.exceptionHandler.exception(this, exc);
		return this;
	}
	
	public Gather<A,R> passExceptions(OpportunisticExceptionHandler handler) {
		if(handler == null) return this;
		ExceptionHandler<A,R> baseHandler = this.exceptionHandler;
		
		this.exceptionHandler = (g, exc)->{
			try {
				handler.exception(exc);
			} catch(Exception xx) {
				baseHandler.exception(g, exc);
			}
		};
		
		return this;
	}
	
	public Collection<A> arguments() {
		return arguments;
	}
	
	public R getValue() {
		return value;
	}
	
	public Gather<A,R> add(Collection<A> argumentList) {
		this.arguments.addAll(argumentList);
		return this;
	}
	
	public Gather<A,R> check(CheckFunction<A,R> function) {
		checkFunctions.add(function);
		return this;
	}
	
	public Gather<A,R> with(WithFunction<A,R> function) {
		withFunctions.put(function, false);
		return this;
	}
	
	public Gather<A,R> find(R defaultValue, GatherVoidFunction<A,R> voidFunction) {
		this.value = defaultValue;
		this.function = (gather, arg) -> {
			voidFunction.invoke(gather, arg);
			return null;
		};
		
		return this;
	}
	
	public Gather<A,R> map(GatherFunction<A,R> function) {
		this.function = (gather, arg) -> {
			return this.value = function.invoke(gather, arg);
		};
		
		return this;
	}
	
	public Gather<A,R> map(GatherFunctionAsync<A,R> function) {
		return this;
	}
	
	public Gather<A,R> result(GatherFinishedFind<A,R> finished) {
		this.finished = (gather, result) -> finished.finished(gather, result);
		return this;
	}
	
	public Gather<A,R> results(GatherFinishedMap<A,R> finished) {
		this.finished = (gather, result) -> finished.finished(gather, results);
		return this;
	}
	
	public Gather<A,R> restart() {
		// TODO: run all checks, withs and results again
		// (clear argument list too?)
		return this;
	}
	
	public Gather<A,R> run() {
		for(CheckFunction<A,R> checkFunc : checkFunctions) {
			if(stopped) return this;
			try {
				checkFunc.check(this);
			} catch(Exception exc) {
				this.exception(exc);
				if(stopped) return this;
			}
		}
		
		for(WithFunction<A,R> withFunc : withFunctions.keySet()) {
			if(stopped) return this;
			try {
				withFunc.with(this, (args)->{
					if(stopped)      return;
					if(args != null) add(args);
					withFunctions.remove(withFunc);
					checkWithComplete();
				});
			} catch(Exception exc) {
				this.exception(exc);
				if(stopped) return this;
			}
		}
		
		checkWithComplete();
		
		return this;
	}
	
	public Gather<A,R> runInParallel() {
		// TODO: let multiple worker threads handle each task
		return this;
	}
	
	public Gather<A,R> finish (R result) {
	  if(stopped)     return this;
	  
	  synchronized(this)
	  {
		if(stopped)   return this;
		value       = result;
		stopped     =   true;
	  }
	  
	  try {
		  finished.finished(this, value);
	  } catch(Exception exc) {
		  this.exception(exc);
	  }
	  
	  return this;
	}
	
	protected void checkWithComplete() {
		if(stopped                 ) return;
		if(withFunctions.size() > 0) return;
		withComplete = true;
	}
	
	protected void runWithArgument(A argument) {
		if(stopped) return;
		synchronized(this) {
			if(stopped) return;
			numPending++;
		}
		
		try {
			R result = function.invoke(this, argument);
			results.put(argument, result);
		} catch(Exception exc) {
			this.exception(exc);
		}
		
		if(stopped) return;
		synchronized(this) {
			if(stopped) return;
			numPending--;
		}

		if(numPending == 0) {
			finish(value);
		}
	}
}
