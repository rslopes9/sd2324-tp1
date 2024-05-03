package tukano.impl.grpc.servers;


import java.io.IOException;
import java.util.logging.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import tukano.impl.discovery.Discovery;
import tukano.impl.java.servers.AbstractServer;
import utils.IP;


public class AbstractGrpcServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "grpc://%s:%s%s";

	private static final String GRPC_CTX = "/grpc";

	protected final Server server;

	protected AbstractGrpcServer(Logger log, String service, int port, AbstractGrpcStub stub) {
		super(log, service, String.format(SERVER_BASE_URI, IP.hostAddress(), port, GRPC_CTX));
		this.server = ServerBuilder.forPort(port).addService(stub).build();
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
