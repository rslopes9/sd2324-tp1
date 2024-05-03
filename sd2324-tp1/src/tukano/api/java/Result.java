package tukano.api.java;

import java.util.function.Function;

/**
 * 
 * Represents the result of an operation, either wrapping a result of the given type,
 * or an error.
 * 
 * @author smd
 *
 * @param <T> type of the result value associated with success
 */
public interface Result<T> {

	/**
	 * 
	 * @author smd
	 *
	 * Service errors:
	 * OK - no error, implies a non-null result of type T, except for for Void operations
	 * CONFLICT - something is being created but already exists
	 * NOT_FOUND - an access occurred to something that does not exist
	 * INTERNAL_ERROR - something unexpected happened
	 */
	enum ErrorCode{ OK, CONFLICT, NOT_FOUND, BAD_REQUEST, FORBIDDEN, INTERNAL_ERROR, NOT_IMPLEMENTED, TIMEOUT};
	
	/**
	 * Tests if the result is an error.
	 */
	boolean isOK();
	
	/**
	 * obtains the payload value of this result
	 * @return the value of this result.
	 */
	T value();

	/**
	 *
	 * obtains the error code of this result
	 * @return the error code
	 * 
	 */
	ErrorCode error();
	
	/**
	 * Convenience method for returning non error results of the given type
	 * @param Class of value of the result
	 * @return the value of the result
	 */
	static <T> Result<T> ok( T result ) {
		return new OkResult<>(result);
	}

	/**
	 * Convenience method for returning non error results without a value
	 * @return non-error result
	 */
	static <T> Result<T> ok() {
		return new OkResult<>(null);	
	}
	
	/**
	 * Convenience method used to return an error 
	 * @return
	 */
	static <T> Result<T> error(ErrorCode error) {
		return new ErrorResult<>(error);		
	}
	
	
	static <T> Result<T> errorOrValue( Result<?> res,  T val) {
		if( res.isOK() )
			return ok( val );
		else
			return error( res.error() );
	}
	
	static <T> Result<T> errorOrValue( Result<?> res,  Result<T> other) {
		if( res.isOK() )			
			return other;
		else
			return error( res.error() );
	}
	
	static Result<Void> errorOrVoid( Result<?> res,  Result<?> other) {
		if( res.isOK() )			
			return other.isOK() ? ok() : error( other.error() );
		else
			return error( res.error() );
	}
	
	static <T,Q> Result<Q> errorOrResult( Result<T> a, Function<T, Result<Q>> b) {
		if( a.isOK())
			return b.apply(a.value());
		else
			return error( a.error() );
	}
	
	static <T,Q> Result<Q> errorOrValue( Result<T> a, Function<T, Q> b) {
		if( a.isOK())
			return ok(b.apply(a.value()));
		else
			return error( a.error() );
	}
}

/*
 * 
 */
class OkResult<T> implements Result<T> {

	final T result;
	
	OkResult(T result) {
		this.result = result;
	}
	
	@Override
	public boolean isOK() {
		return true;
	}

	@Override
	public T value() {
		return result;
	}

	@Override
	public ErrorCode error() {
		return ErrorCode.OK;
	}
	
	public String toString() {
		return "(OK, " + value() + ")";
	}
}

class ErrorResult<T> implements Result<T> {

	final ErrorCode error;
	
	ErrorResult(ErrorCode error) {
		this.error = error;
	}
	
	@Override
	public boolean isOK() {
		return false;
	}

	@Override
	public T value() {
		throw new RuntimeException("Attempting to extract the value of an Error: " + error());
	}

	@Override
	public ErrorCode error() {
		return error;
	}
	
	public String toString() {
		return "(" + error() + ")";		
	}
}
