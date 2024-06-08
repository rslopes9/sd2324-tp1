package tukano.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tukano.api.java.Blobs;
import tukano.impl.rest.servers.utils.CustomLoggingFilter;
import tukano.impl.rest.servers.utils.GenericExceptionMapper;
import utils.Args;

public class RestBlobsProxyServer extends AbstractRestServer {
	public static final int PORT = 4567;
	
	private static Logger Log = Logger.getLogger(RestBlobsProxyServer.class.getName());
	
    static String apiKey;
    static String apiSecret;
    static String accessTokenStr;
    static boolean ignoreState;


	RestBlobsProxyServer(int port) {
		super( Log, Blobs.NAME, port);
	}
	
	
	@Override
	void registerResources(ResourceConfig config) {
		config.register(new RestBlobsProxyResource(apiKey,apiSecret,accessTokenStr,ignoreState)); 
		config.register(new GenericExceptionMapper());
		config.register(new CustomLoggingFilter());
	}
	
	public static void main(String[] args) {
		Args.use(args);
		
		apiKey = Args.valueOf("-apiKey", "");
		apiSecret = Args.valueOf("-apiSecret", "");
        accessTokenStr = Args.valueOf("-accessTokenStr", "");
        
        
        if(args[0].equals("true")){
        	ignoreState = true;
        }
        else {
        	ignoreState = false;
        }
       
		new RestBlobsProxyServer(Args.valueOf("-port", PORT)).start();
	}	

}
