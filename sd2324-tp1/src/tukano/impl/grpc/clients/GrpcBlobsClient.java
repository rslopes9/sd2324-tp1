package tukano.impl.grpc.clients;

import java.io.ByteArrayOutputStream;
import java.util.function.Consumer;

import com.google.protobuf.ByteString;

import tukano.api.java.Result;
import tukano.impl.api.java.ExtendedBlobs;
import tukano.impl.grpc.generated_java.BlobsGrpc;
import tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs;
import tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs;
import tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs;
import tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs;

public class GrpcBlobsClient extends GrpcClient implements ExtendedBlobs {

	final BlobsGrpc.BlobsBlockingStub stub;

	public GrpcBlobsClient(String serverURI) {
		super(serverURI);
		this.stub = BlobsGrpc.newBlockingStub( super.channel );
	}

	
	
	@Override
	public Result<Void> upload(String blobId, byte[] bytes) {
		return super.toJavaResult(() -> {
			stub.upload( UploadArgs.newBuilder()
				.setBlobId( blobId )
				.setData( ByteString.copyFrom(bytes))
				.build());

		});
	}

	@Override
	public Result<byte[]> download(String blobId) {
		return super.toJavaResult(() -> {
			var res = stub.download( DownloadArgs.newBuilder()
				.setBlobId(blobId)
				.build());			
			var baos = new ByteArrayOutputStream();
			res.forEachRemaining( part -> {
				baos.writeBytes( part.getChunk().toByteArray() );
			});
			return baos.toByteArray();
		});
	}

	public Result<Void> downloadToSink(String blobId, Consumer<byte[]> sink) {
		return super.toJavaResult(() -> {
			var res = stub.download( DownloadArgs.newBuilder()
				.setBlobId(blobId)
				.build());
			
			res.forEachRemaining( (part) -> sink.accept( part.getChunk().toByteArray()));	
		});
	}
	
	@Override
	public Result<Void> deleteAllBlobs(String userId, String token) {
		return super.toJavaResult(() -> {
			stub.deleteAllBlobs( DeleteAllBlobsArgs.newBuilder()
				.setUserId(userId)
				.setToken( token)
				.build());			
		});	
	}
	
	@Override
	public Result<Void> delete(String blobId, String token) {
		return super.toJavaResult(() -> {
			stub.delete( DeleteArgs.newBuilder()
				.setBlobId(blobId)
				.setToken(token)
				.build());			
		});	
	}
}
