package tukano.impl.grpc.servers;

import com.google.protobuf.ByteString;

import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import tukano.api.java.Blobs;
import tukano.impl.grpc.generated_java.BlobsGrpc;
import tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs;
import tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult;
import tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs;
import tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult;
import tukano.impl.java.servers.JavaBlobs;

public class GrpcBlobsServerStub extends AbstractGrpcStub implements BlobsGrpc.AsyncService {

	Blobs impl = new JavaBlobs();

	@Override
	public ServerServiceDefinition bindService() {
		return BlobsGrpc.bindService(this);
	}

	//@Override
	public void upload(UploadArgs request, StreamObserver<UploadResult> responseObserver) {
		var res = impl.upload(request.getBlobId(), request.getData().toByteArray());
		if (!res.isOK())
			responseObserver.onError(errorCodeToStatus(res.error()));
		else {
			responseObserver.onNext(UploadResult.newBuilder().build());
			responseObserver.onCompleted();
		}
	}

	public void download(DownloadArgs request, StreamObserver<DownloadResult> responseObserver) {
		var res = impl.downloadToSink(request.getBlobId(), (data) -> {
			responseObserver.onNext(DownloadResult.newBuilder().setChunk(ByteString.copyFrom(data)).build());
		});
		if (res.isOK())
			responseObserver.onCompleted();
		else
			responseObserver.onError(errorCodeToStatus(res.error()));
	}
}
