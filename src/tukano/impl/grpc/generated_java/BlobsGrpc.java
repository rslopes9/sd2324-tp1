package tukano.impl.grpc.generated_java;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.62.2)",
    comments = "Source: src/tukano/api/grpc/Blobs.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class BlobsGrpc {

  private BlobsGrpc() {}

  public static final java.lang.String SERVICE_NAME = "Blobs";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs,
      tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult> getUploadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "upload",
      requestType = tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs.class,
      responseType = tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs,
      tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult> getUploadMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs, tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult> getUploadMethod;
    if ((getUploadMethod = BlobsGrpc.getUploadMethod) == null) {
      synchronized (BlobsGrpc.class) {
        if ((getUploadMethod = BlobsGrpc.getUploadMethod) == null) {
          BlobsGrpc.getUploadMethod = getUploadMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs, tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "upload"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult.getDefaultInstance()))
              .setSchemaDescriptor(new BlobsMethodDescriptorSupplier("upload"))
              .build();
        }
      }
    }
    return getUploadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs,
      tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult> getDownloadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "download",
      requestType = tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs.class,
      responseType = tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs,
      tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult> getDownloadMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs, tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult> getDownloadMethod;
    if ((getDownloadMethod = BlobsGrpc.getDownloadMethod) == null) {
      synchronized (BlobsGrpc.class) {
        if ((getDownloadMethod = BlobsGrpc.getDownloadMethod) == null) {
          BlobsGrpc.getDownloadMethod = getDownloadMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs, tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "download"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult.getDefaultInstance()))
              .setSchemaDescriptor(new BlobsMethodDescriptorSupplier("download"))
              .build();
        }
      }
    }
    return getDownloadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs,
      tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult> getDeleteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "delete",
      requestType = tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs.class,
      responseType = tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs,
      tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult> getDeleteMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs, tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult> getDeleteMethod;
    if ((getDeleteMethod = BlobsGrpc.getDeleteMethod) == null) {
      synchronized (BlobsGrpc.class) {
        if ((getDeleteMethod = BlobsGrpc.getDeleteMethod) == null) {
          BlobsGrpc.getDeleteMethod = getDeleteMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs, tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "delete"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult.getDefaultInstance()))
              .setSchemaDescriptor(new BlobsMethodDescriptorSupplier("delete"))
              .build();
        }
      }
    }
    return getDeleteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs,
      tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult> getDeleteAllBlobsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "deleteAllBlobs",
      requestType = tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs.class,
      responseType = tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs,
      tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult> getDeleteAllBlobsMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs, tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult> getDeleteAllBlobsMethod;
    if ((getDeleteAllBlobsMethod = BlobsGrpc.getDeleteAllBlobsMethod) == null) {
      synchronized (BlobsGrpc.class) {
        if ((getDeleteAllBlobsMethod = BlobsGrpc.getDeleteAllBlobsMethod) == null) {
          BlobsGrpc.getDeleteAllBlobsMethod = getDeleteAllBlobsMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs, tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "deleteAllBlobs"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult.getDefaultInstance()))
              .setSchemaDescriptor(new BlobsMethodDescriptorSupplier("deleteAllBlobs"))
              .build();
        }
      }
    }
    return getDeleteAllBlobsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BlobsStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BlobsStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BlobsStub>() {
        @java.lang.Override
        public BlobsStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BlobsStub(channel, callOptions);
        }
      };
    return BlobsStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BlobsBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BlobsBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BlobsBlockingStub>() {
        @java.lang.Override
        public BlobsBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BlobsBlockingStub(channel, callOptions);
        }
      };
    return BlobsBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BlobsFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BlobsFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BlobsFutureStub>() {
        @java.lang.Override
        public BlobsFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BlobsFutureStub(channel, callOptions);
        }
      };
    return BlobsFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void upload(tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUploadMethod(), responseObserver);
    }

    /**
     */
    default void download(tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDownloadMethod(), responseObserver);
    }

    /**
     */
    default void delete(tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteMethod(), responseObserver);
    }

    /**
     */
    default void deleteAllBlobs(tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteAllBlobsMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service Blobs.
   */
  public static abstract class BlobsImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return BlobsGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service Blobs.
   */
  public static final class BlobsStub
      extends io.grpc.stub.AbstractAsyncStub<BlobsStub> {
    private BlobsStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlobsStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BlobsStub(channel, callOptions);
    }

    /**
     */
    public void upload(tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUploadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void download(tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getDownloadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void delete(tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteAllBlobs(tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteAllBlobsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service Blobs.
   */
  public static final class BlobsBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<BlobsBlockingStub> {
    private BlobsBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlobsBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BlobsBlockingStub(channel, callOptions);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult upload(tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUploadMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult> download(
        tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getDownloadMethod(), getCallOptions(), request);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult delete(tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteMethod(), getCallOptions(), request);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult deleteAllBlobs(tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteAllBlobsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service Blobs.
   */
  public static final class BlobsFutureStub
      extends io.grpc.stub.AbstractFutureStub<BlobsFutureStub> {
    private BlobsFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlobsFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BlobsFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult> upload(
        tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUploadMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult> delete(
        tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult> deleteAllBlobs(
        tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteAllBlobsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_UPLOAD = 0;
  private static final int METHODID_DOWNLOAD = 1;
  private static final int METHODID_DELETE = 2;
  private static final int METHODID_DELETE_ALL_BLOBS = 3;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_UPLOAD:
          serviceImpl.upload((tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult>) responseObserver);
          break;
        case METHODID_DOWNLOAD:
          serviceImpl.download((tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult>) responseObserver);
          break;
        case METHODID_DELETE:
          serviceImpl.delete((tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult>) responseObserver);
          break;
        case METHODID_DELETE_ALL_BLOBS:
          serviceImpl.deleteAllBlobs((tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getUploadMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadArgs,
              tukano.impl.grpc.generated_java.BlobsProtoBuf.UploadResult>(
                service, METHODID_UPLOAD)))
        .addMethod(
          getDownloadMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadArgs,
              tukano.impl.grpc.generated_java.BlobsProtoBuf.DownloadResult>(
                service, METHODID_DOWNLOAD)))
        .addMethod(
          getDeleteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteArgs,
              tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteResult>(
                service, METHODID_DELETE)))
        .addMethod(
          getDeleteAllBlobsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsArgs,
              tukano.impl.grpc.generated_java.BlobsProtoBuf.DeleteAllBlobsResult>(
                service, METHODID_DELETE_ALL_BLOBS)))
        .build();
  }

  private static abstract class BlobsBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BlobsBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return tukano.impl.grpc.generated_java.BlobsProtoBuf.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Blobs");
    }
  }

  private static final class BlobsFileDescriptorSupplier
      extends BlobsBaseDescriptorSupplier {
    BlobsFileDescriptorSupplier() {}
  }

  private static final class BlobsMethodDescriptorSupplier
      extends BlobsBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    BlobsMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (BlobsGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BlobsFileDescriptorSupplier())
              .addMethod(getUploadMethod())
              .addMethod(getDownloadMethod())
              .addMethod(getDeleteMethod())
              .addMethod(getDeleteAllBlobsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
