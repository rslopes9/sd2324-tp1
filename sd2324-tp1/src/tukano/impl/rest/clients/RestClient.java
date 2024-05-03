package tukano.impl.rest.clients;


import static tukano.api.java.Result.error;
import static tukano.api.java.Result.ok;
import static tukano.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.java.Result.ErrorCode.TIMEOUT;

import java.util.function.Supplier;
import java.util.logging.Logger;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import tukano.api.java.Result;
import tukano.api.java.Result.ErrorCode;
import utils.Sleep;

public class RestClient {
	private static Logger Log = Logger.getLogger(RestClient.class.getName());

	protected static final int READ_TIMEOUT = 10000;
	protected static final int CONNECT_TIMEOUT = 10000;

	protected static final int MAX_RETRIES = 3;
	protected static final int RETRY_SLEEP = 1000;

	final Client client;
	final String serverURI;
	final ClientConfig config;

	final WebTarget target;
	
	protected RestClient(String serverURI, String servicePath ) {
		this.serverURI = serverURI;
		this.config = new ClientConfig();

		config.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);

		this.client = ClientBuilder.newClient(config);
		this.target = client.target( serverURI ).path( servicePath );
	}

	protected <T> Result<T> reTry(Supplier<Result<T>> func) {
		for (int i = 0; i < MAX_RETRIES; i++)
			try {
				return func.get();
			} catch (ProcessingException x) {
				Log.fine("Timeout: " + x.getMessage());
				Sleep.ms(RETRY_SLEEP);
			} catch (Exception x) {
				x.printStackTrace();
				return Result.error(INTERNAL_ERROR);
			}
		System.err.println("TIMEOUT...");
		return Result.error(TIMEOUT);
	}

	protected Result<Void> toJavaResult(Response r) {
		try {
			var status = r.getStatusInfo().toEnum();
			if (status == Status.OK && r.hasEntity()) {
				return ok(null);
			}
			else 
				if( status == Status.NO_CONTENT) return ok();
			
			return error(getErrorCodeFrom(status.getStatusCode()));
		} finally {
			r.close();
		}
	}

	protected <T> Result<T> toJavaResult(Response r, Class<T> entityType) {
		try {
			var status = r.getStatusInfo().toEnum();
			if (status == Status.OK && r.hasEntity())
				return ok(r.readEntity(entityType));
			else 
				if( status == Status.NO_CONTENT) return ok();
			
			return error(getErrorCodeFrom(status.getStatusCode()));
		} finally {
			r.close();
		}
	}
	
	protected <T> Result<T> toJavaResult(Response r, GenericType<T> entityType) {
		try {
			var status = r.getStatusInfo().toEnum();
			if (status == Status.OK && r.hasEntity())
				return ok(r.readEntity(entityType));
			else 
				if( status == Status.NO_CONTENT) return ok();
			
			return error(getErrorCodeFrom(status.getStatusCode()));
		} finally {
			r.close();
		}
	}
	
	public static ErrorCode getErrorCodeFrom(int status) {
		return switch (status) {
		case 200, 204 -> ErrorCode.OK;
		case 409 -> ErrorCode.CONFLICT;
		case 403 -> ErrorCode.FORBIDDEN;
		case 404 -> ErrorCode.NOT_FOUND;
		case 400 -> ErrorCode.BAD_REQUEST;
		case 500 -> ErrorCode.INTERNAL_ERROR;
		case 501 -> ErrorCode.NOT_IMPLEMENTED;
		default -> ErrorCode.INTERNAL_ERROR;
		};
	}

	@Override
	public String toString() {
		return serverURI.toString();
	}
}
