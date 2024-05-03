package tukano.impl.grpc.clients;

import static tukano.api.java.Result.error;
import static tukano.api.java.Result.ok;
import static tukano.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.java.Result.ErrorCode.TIMEOUT;

import java.net.URI;
import java.util.function.Supplier;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.Status.Code;
import tukano.api.java.Result;
import tukano.api.java.Result.ErrorCode;

public class GrpcClient {

	final protected URI serverURI;
	final protected Channel channel;

	protected GrpcClient(String serverUrl) {
		this.serverURI = URI.create(serverUrl);
		this.channel = ManagedChannelBuilder.forAddress(serverURI.getHost(), serverURI.getPort())
				.usePlaintext().enableRetry().build();
	}
	
	protected <T> Result<T> toJavaResult(Supplier<T> func) {
		try {
			return ok(func.get());
		} catch (StatusRuntimeException sre) {
			return error(statusToErrorCode(sre.getStatus()));
		} catch (Exception x) {
			x.printStackTrace();
			return Result.error(INTERNAL_ERROR);
		}
	}

	protected Result<Void> toJavaResult(Runnable proc) {
		return toJavaResult( () -> {
			proc.run();
			return null;
		} );		
	}

	protected static ErrorCode statusToErrorCode(Status status) {
		return switch (status.getCode()) {
		case OK -> ErrorCode.OK;
		case NOT_FOUND -> ErrorCode.NOT_FOUND;
		case ALREADY_EXISTS -> ErrorCode.CONFLICT;
		case PERMISSION_DENIED -> ErrorCode.FORBIDDEN;
		case INVALID_ARGUMENT -> ErrorCode.BAD_REQUEST;
		case UNIMPLEMENTED -> ErrorCode.NOT_IMPLEMENTED;
		default -> ErrorCode.INTERNAL_ERROR;
		};
	}
	
	@Override
	public String toString() {
		return serverURI.toString();
	}
}

