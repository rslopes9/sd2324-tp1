package tukano.impl.java.clients;

import tukano.api.java.Blobs;
import tukano.api.java.Shorts;
import tukano.api.java.Users;
import tukano.impl.api.java.ExtendedBlobs;
import tukano.impl.api.java.ExtendedShorts;
import tukano.impl.grpc.clients.GrpcBlobsClient;
import tukano.impl.grpc.clients.GrpcShortsClient;
import tukano.impl.grpc.clients.GrpcUsersClient;
import tukano.impl.rest.clients.RestBlobsClient;
import tukano.impl.rest.clients.RestShortsClient;
import tukano.impl.rest.clients.RestUsersClient;

public class Clients {
	public static final ClientFactory<Users> UsersClients = new ClientFactory<>(Users.NAME, RestUsersClient::new, GrpcUsersClient::new);
	
	public static final ClientFactory<ExtendedBlobs> BlobsClients = new ClientFactory<>(Blobs.NAME, RestBlobsClient::new, GrpcBlobsClient::new);

	public static final ClientFactory<ExtendedShorts> ShortsClients = new ClientFactory<>(Shorts.NAME, RestShortsClient::new, GrpcShortsClient::new);	
}
