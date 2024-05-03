package tukano.impl.rest.clients;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import tukano.api.java.Result;
import tukano.api.rest.RestBlobs;
import tukano.impl.api.java.ExtendedBlobs;
import tukano.impl.api.rest.RestExtendedBlobs;

public class RestBlobsClient extends RestClient implements ExtendedBlobs {

	public RestBlobsClient(String serverURI) {
		super(serverURI, RestBlobs.PATH);
	}

	private Result<Void> _upload(String blobId, byte[] bytes) {
		return super.toJavaResult(
				target.path(blobId)
				.request()
				.post( Entity.entity(bytes, MediaType.APPLICATION_OCTET_STREAM)));
	}

	private Result<byte[]> _download(String blobId) {
		return super.toJavaResult(
				target.path(blobId)
				.request()
				.accept(MediaType.APPLICATION_OCTET_STREAM)
				.get(), byte[].class);
	}

	private Result<Void> _delete(String blobURL, String token) {
		return super.toJavaResult(
				client.target( blobURL ).path( blobURL )
				.queryParam( RestExtendedBlobs.TOKEN, token )
				.request()
				.delete());
	}
	
	private Result<Void> _deleteAllBlobs(String userId, String token) {
		return super.toJavaResult(
				target.path(userId)
				.path(RestExtendedBlobs.BLOBS)
				.queryParam( RestExtendedBlobs.TOKEN, token )
				.request()
				.delete());
	}
	
	@Override
	public Result<Void> upload(String blobId, byte[] bytes) {
		return super.reTry( () -> _upload(blobId, bytes));
	}

	@Override
	public Result<byte[]> download(String blobId) {
		return super.reTry( () -> _download(blobId));
	}

	@Override
	public Result<Void> delete(String blobId, String token) {
		return super.reTry( () -> _delete(blobId, token));
	}
	
	@Override
	public Result<Void> deleteAllBlobs(String userId, String password) {
		return super.reTry( () -> _deleteAllBlobs(userId, password));
	}
}
