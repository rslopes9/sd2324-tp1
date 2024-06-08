package tukano.impl.grpc.servers;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import tukano.impl.discovery.Discovery;
import tukano.impl.java.servers.AbstractServer;
import utils.IP;


public class AbstractGrpcServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "grpc://%s:%s%s";

	private static final String GRPC_CTX = "/grpc";

	protected final Server server;


	protected AbstractGrpcServer(Logger log, String service, int port, AbstractGrpcStub stub)  {
		super(log, service, String.format(SERVER_BASE_URI, IP.hostname(), port, GRPC_CTX));
		
		String keyStore = System.getProperty("javax.net.ssl.keyStore");
		String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
	    
	
				KeyStore keystore = null;
				try {
					keystore = KeyStore.getInstance(KeyStore.getDefaultType());
				} catch (KeyStoreException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			  FileInputStream in = null;
			  try {
				in = new FileInputStream(keyStore);
			  } catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			  }
			    	try {
						keystore.load(in, keyStorePassword.toCharArray());
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
			    
			    
			    
			    KeyManagerFactory keyManagerFactory = null;
				try {
					keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
				} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    try {
					keyManagerFactory.init(keystore, keyStorePassword.toCharArray());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			    
			    SslContext sslContext = null;
				try {
					sslContext = GrpcSslContexts.configure(SslContextBuilder.forServer(keyManagerFactory)).build();
				} catch (SSLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			    
			    
		
		
			this.server = NettyServerBuilder.forPort(port).addService(stub).sslContext(sslContext).build();

	}

	protected void start() throws IOException {
		
		Discovery.getInstance().announce(service, super.serverURI);
		
		Log.info(String.format("%s gRPC Server ready @ %s\n", service, serverURI));

		server.start();
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			System.err.println("*** shutting down gRPC server since JVM is shutting down");
			server.shutdownNow();
			System.err.println("*** server shut down");
		}));
	}
	
}
