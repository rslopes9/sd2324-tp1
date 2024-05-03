package tukano.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tukano.api.java.Blobs;
import tukano.impl.rest.servers.utils.CustomLoggingFilter;
import tukano.impl.rest.servers.utils.GenericExceptionMapper;
import utils.Args;


public class RestBlobsServer extends AbstractRestServer {
	public static final int PORT = 5678;
	
	private static Logger Log = Logger.getLogger(RestBlobsServer.class.getName());

	RestBlobsServer(int port) {
		super( Log, Blobs.NAME, port);
	}
	
	
	@Override
	void registerResources(ResourceConfig config) {
		config.register( RestBlobsResource.class ); 
		config.register(new GenericExceptionMapper());
		config.register(new CustomLoggingFilter());
	}
	
	public static void main(String[] args) {
		Args.use(args);
		new RestBlobsServer(Args.valueOf("-port", PORT)).start();
	}	
}