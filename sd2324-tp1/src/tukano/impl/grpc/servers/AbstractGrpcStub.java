package tukano.impl.grpc.servers;

import io.grpc.BindableService;
import tukano.api.java.Result;

public abstract class AbstractGrpcStub implements BindableService {

	protected static Throwable errorCodeToStatus( Result.ErrorCode error ) {
    	var status =  switch( error) {
    	case NOT_FOUND -> io.grpc.Status.NOT_FOUND; 
    	case CONFLICT -> io.grpc.Status.ALREADY_EXISTS;
    	case FORBIDDEN -> io.grpc.Status.PERMISSION_DENIED;
    	case NOT_IMPLEMENTED -> io.grpc.Status.UNIMPLEMENTED;
    	case BAD_REQUEST -> io.grpc.Status.INVALID_ARGUMENT;
    	default -> io.grpc.Status.INTERNAL;
    	};
    	
    	return status.asException();
    }
}
