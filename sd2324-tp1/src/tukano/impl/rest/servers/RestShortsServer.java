package tukano.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tukano.api.java.Shorts;
import tukano.impl.rest.servers.utils.CustomLoggingFilter;
import tukano.impl.rest.servers.utils.GenericExceptionMapper;
import utils.Args;


public class RestShortsServer extends AbstractRestServer {
	public static final int PORT = 4567;
	
	private static Logger Log = Logger.getLogger(RestShortsServer.class.getName());

	RestShortsServer() {
		super( Log, Shorts.NAME, PORT);
	}
	
	
	@Override
	void registerResources(ResourceConfig config) {
		config.register( RestShortsResource.class ); 
		config.register(new GenericExceptionMapper());
		config.register(new CustomLoggingFilter());
	}
	
	public static void main(String[] args) {
		Args.use(args);
		new RestShortsServer().start();
	}	
}