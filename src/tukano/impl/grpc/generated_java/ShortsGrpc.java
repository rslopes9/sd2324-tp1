package tukano.impl.grpc.generated_java;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.62.2)",
    comments = "Source: src/tukano/api/grpc/Shorts.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ShortsGrpc {

  private ShortsGrpc() {}

  public static final java.lang.String SERVICE_NAME = "Shorts";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult> getCreateShortMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "createShort",
      requestType = tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs.class,
      responseType = tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult> getCreateShortMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult> getCreateShortMethod;
    if ((getCreateShortMethod = ShortsGrpc.getCreateShortMethod) == null) {
      synchronized (ShortsGrpc.class) {
        if ((getCreateShortMethod = ShortsGrpc.getCreateShortMethod) == null) {
          ShortsGrpc.getCreateShortMethod = getCreateShortMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "createShort"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult.getDefaultInstance()))
              .setSchemaDescriptor(new ShortsMethodDescriptorSupplier("createShort"))
              .build();
        }
      }
    }
    return getCreateShortMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult> getDeleteShortMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "deleteShort",
      requestType = tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs.class,
      responseType = tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult> getDeleteShortMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult> getDeleteShortMethod;
    if ((getDeleteShortMethod = ShortsGrpc.getDeleteShortMethod) == null) {
      synchronized (ShortsGrpc.class) {
        if ((getDeleteShortMethod = ShortsGrpc.getDeleteShortMethod) == null) {
          ShortsGrpc.getDeleteShortMethod = getDeleteShortMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "deleteShort"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult.getDefaultInstance()))
              .setSchemaDescriptor(new ShortsMethodDescriptorSupplier("deleteShort"))
              .build();
        }
      }
    }
    return getDeleteShortMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult> getGetShortMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "getShort",
      requestType = tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs.class,
      responseType = tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult> getGetShortMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult> getGetShortMethod;
    if ((getGetShortMethod = ShortsGrpc.getGetShortMethod) == null) {
      synchronized (ShortsGrpc.class) {
        if ((getGetShortMethod = ShortsGrpc.getGetShortMethod) == null) {
          ShortsGrpc.getGetShortMethod = getGetShortMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "getShort"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult.getDefaultInstance()))
              .setSchemaDescriptor(new ShortsMethodDescriptorSupplier("getShort"))
              .build();
        }
      }
    }
    return getGetShortMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult> getGetShortsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "getShorts",
      requestType = tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs.class,
      responseType = tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult> getGetShortsMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult> getGetShortsMethod;
    if ((getGetShortsMethod = ShortsGrpc.getGetShortsMethod) == null) {
      synchronized (ShortsGrpc.class) {
        if ((getGetShortsMethod = ShortsGrpc.getGetShortsMethod) == null) {
          ShortsGrpc.getGetShortsMethod = getGetShortsMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "getShorts"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult.getDefaultInstance()))
              .setSchemaDescriptor(new ShortsMethodDescriptorSupplier("getShorts"))
              .build();
        }
      }
    }
    return getGetShortsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult> getFollowMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "follow",
      requestType = tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs.class,
      responseType = tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult> getFollowMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult> getFollowMethod;
    if ((getFollowMethod = ShortsGrpc.getFollowMethod) == null) {
      synchronized (ShortsGrpc.class) {
        if ((getFollowMethod = ShortsGrpc.getFollowMethod) == null) {
          ShortsGrpc.getFollowMethod = getFollowMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "follow"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult.getDefaultInstance()))
              .setSchemaDescriptor(new ShortsMethodDescriptorSupplier("follow"))
              .build();
        }
      }
    }
    return getFollowMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult> getFollowersMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "followers",
      requestType = tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs.class,
      responseType = tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult> getFollowersMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult> getFollowersMethod;
    if ((getFollowersMethod = ShortsGrpc.getFollowersMethod) == null) {
      synchronized (ShortsGrpc.class) {
        if ((getFollowersMethod = ShortsGrpc.getFollowersMethod) == null) {
          ShortsGrpc.getFollowersMethod = getFollowersMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "followers"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult.getDefaultInstance()))
              .setSchemaDescriptor(new ShortsMethodDescriptorSupplier("followers"))
              .build();
        }
      }
    }
    return getFollowersMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult> getLikeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "like",
      requestType = tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs.class,
      responseType = tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult> getLikeMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult> getLikeMethod;
    if ((getLikeMethod = ShortsGrpc.getLikeMethod) == null) {
      synchronized (ShortsGrpc.class) {
        if ((getLikeMethod = ShortsGrpc.getLikeMethod) == null) {
          ShortsGrpc.getLikeMethod = getLikeMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "like"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult.getDefaultInstance()))
              .setSchemaDescriptor(new ShortsMethodDescriptorSupplier("like"))
              .build();
        }
      }
    }
    return getLikeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult> getLikesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "likes",
      requestType = tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs.class,
      responseType = tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult> getLikesMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult> getLikesMethod;
    if ((getLikesMethod = ShortsGrpc.getLikesMethod) == null) {
      synchronized (ShortsGrpc.class) {
        if ((getLikesMethod = ShortsGrpc.getLikesMethod) == null) {
          ShortsGrpc.getLikesMethod = getLikesMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "likes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult.getDefaultInstance()))
              .setSchemaDescriptor(new ShortsMethodDescriptorSupplier("likes"))
              .build();
        }
      }
    }
    return getLikesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult> getGetFeedMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "getFeed",
      requestType = tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs.class,
      responseType = tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult> getGetFeedMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult> getGetFeedMethod;
    if ((getGetFeedMethod = ShortsGrpc.getGetFeedMethod) == null) {
      synchronized (ShortsGrpc.class) {
        if ((getGetFeedMethod = ShortsGrpc.getGetFeedMethod) == null) {
          ShortsGrpc.getGetFeedMethod = getGetFeedMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "getFeed"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult.getDefaultInstance()))
              .setSchemaDescriptor(new ShortsMethodDescriptorSupplier("getFeed"))
              .build();
        }
      }
    }
    return getGetFeedMethod;
  }

  private static volatile io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult> getDeleteAllShortsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "deleteAllShorts",
      requestType = tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs.class,
      responseType = tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs,
      tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult> getDeleteAllShortsMethod() {
    io.grpc.MethodDescriptor<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult> getDeleteAllShortsMethod;
    if ((getDeleteAllShortsMethod = ShortsGrpc.getDeleteAllShortsMethod) == null) {
      synchronized (ShortsGrpc.class) {
        if ((getDeleteAllShortsMethod = ShortsGrpc.getDeleteAllShortsMethod) == null) {
          ShortsGrpc.getDeleteAllShortsMethod = getDeleteAllShortsMethod =
              io.grpc.MethodDescriptor.<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs, tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "deleteAllShorts"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult.getDefaultInstance()))
              .setSchemaDescriptor(new ShortsMethodDescriptorSupplier("deleteAllShorts"))
              .build();
        }
      }
    }
    return getDeleteAllShortsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ShortsStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ShortsStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ShortsStub>() {
        @java.lang.Override
        public ShortsStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ShortsStub(channel, callOptions);
        }
      };
    return ShortsStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ShortsBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ShortsBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ShortsBlockingStub>() {
        @java.lang.Override
        public ShortsBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ShortsBlockingStub(channel, callOptions);
        }
      };
    return ShortsBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ShortsFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ShortsFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ShortsFutureStub>() {
        @java.lang.Override
        public ShortsFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ShortsFutureStub(channel, callOptions);
        }
      };
    return ShortsFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void createShort(tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateShortMethod(), responseObserver);
    }

    /**
     */
    default void deleteShort(tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteShortMethod(), responseObserver);
    }

    /**
     */
    default void getShort(tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetShortMethod(), responseObserver);
    }

    /**
     */
    default void getShorts(tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetShortsMethod(), responseObserver);
    }

    /**
     */
    default void follow(tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFollowMethod(), responseObserver);
    }

    /**
     */
    default void followers(tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFollowersMethod(), responseObserver);
    }

    /**
     */
    default void like(tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLikeMethod(), responseObserver);
    }

    /**
     */
    default void likes(tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLikesMethod(), responseObserver);
    }

    /**
     */
    default void getFeed(tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetFeedMethod(), responseObserver);
    }

    /**
     */
    default void deleteAllShorts(tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteAllShortsMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service Shorts.
   */
  public static abstract class ShortsImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ShortsGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service Shorts.
   */
  public static final class ShortsStub
      extends io.grpc.stub.AbstractAsyncStub<ShortsStub> {
    private ShortsStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShortsStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ShortsStub(channel, callOptions);
    }

    /**
     */
    public void createShort(tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateShortMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteShort(tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteShortMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getShort(tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetShortMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getShorts(tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetShortsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void follow(tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFollowMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void followers(tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFollowersMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void like(tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLikeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void likes(tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLikesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getFeed(tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetFeedMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteAllShorts(tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs request,
        io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteAllShortsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service Shorts.
   */
  public static final class ShortsBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ShortsBlockingStub> {
    private ShortsBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShortsBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ShortsBlockingStub(channel, callOptions);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult createShort(tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateShortMethod(), getCallOptions(), request);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult deleteShort(tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteShortMethod(), getCallOptions(), request);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult getShort(tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetShortMethod(), getCallOptions(), request);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult getShorts(tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetShortsMethod(), getCallOptions(), request);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult follow(tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFollowMethod(), getCallOptions(), request);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult followers(tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFollowersMethod(), getCallOptions(), request);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult like(tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLikeMethod(), getCallOptions(), request);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult likes(tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLikesMethod(), getCallOptions(), request);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult getFeed(tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetFeedMethod(), getCallOptions(), request);
    }

    /**
     */
    public tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult deleteAllShorts(tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteAllShortsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service Shorts.
   */
  public static final class ShortsFutureStub
      extends io.grpc.stub.AbstractFutureStub<ShortsFutureStub> {
    private ShortsFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShortsFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ShortsFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult> createShort(
        tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateShortMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult> deleteShort(
        tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteShortMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult> getShort(
        tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetShortMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult> getShorts(
        tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetShortsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult> follow(
        tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFollowMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult> followers(
        tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFollowersMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult> like(
        tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLikeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult> likes(
        tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLikesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult> getFeed(
        tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetFeedMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult> deleteAllShorts(
        tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteAllShortsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_SHORT = 0;
  private static final int METHODID_DELETE_SHORT = 1;
  private static final int METHODID_GET_SHORT = 2;
  private static final int METHODID_GET_SHORTS = 3;
  private static final int METHODID_FOLLOW = 4;
  private static final int METHODID_FOLLOWERS = 5;
  private static final int METHODID_LIKE = 6;
  private static final int METHODID_LIKES = 7;
  private static final int METHODID_GET_FEED = 8;
  private static final int METHODID_DELETE_ALL_SHORTS = 9;

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
        case METHODID_CREATE_SHORT:
          serviceImpl.createShort((tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult>) responseObserver);
          break;
        case METHODID_DELETE_SHORT:
          serviceImpl.deleteShort((tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult>) responseObserver);
          break;
        case METHODID_GET_SHORT:
          serviceImpl.getShort((tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult>) responseObserver);
          break;
        case METHODID_GET_SHORTS:
          serviceImpl.getShorts((tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult>) responseObserver);
          break;
        case METHODID_FOLLOW:
          serviceImpl.follow((tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult>) responseObserver);
          break;
        case METHODID_FOLLOWERS:
          serviceImpl.followers((tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult>) responseObserver);
          break;
        case METHODID_LIKE:
          serviceImpl.like((tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult>) responseObserver);
          break;
        case METHODID_LIKES:
          serviceImpl.likes((tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult>) responseObserver);
          break;
        case METHODID_GET_FEED:
          serviceImpl.getFeed((tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult>) responseObserver);
          break;
        case METHODID_DELETE_ALL_SHORTS:
          serviceImpl.deleteAllShorts((tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs) request,
              (io.grpc.stub.StreamObserver<tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult>) responseObserver);
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
          getCreateShortMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortArgs,
              tukano.impl.grpc.generated_java.ShortsProtoBuf.CreateShortResult>(
                service, METHODID_CREATE_SHORT)))
        .addMethod(
          getDeleteShortMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortArgs,
              tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteShortResult>(
                service, METHODID_DELETE_SHORT)))
        .addMethod(
          getGetShortMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortArgs,
              tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortResult>(
                service, METHODID_GET_SHORT)))
        .addMethod(
          getGetShortsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsArgs,
              tukano.impl.grpc.generated_java.ShortsProtoBuf.GetShortsResult>(
                service, METHODID_GET_SHORTS)))
        .addMethod(
          getFollowMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowArgs,
              tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowResult>(
                service, METHODID_FOLLOW)))
        .addMethod(
          getFollowersMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersArgs,
              tukano.impl.grpc.generated_java.ShortsProtoBuf.FollowersResult>(
                service, METHODID_FOLLOWERS)))
        .addMethod(
          getLikeMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeArgs,
              tukano.impl.grpc.generated_java.ShortsProtoBuf.LikeResult>(
                service, METHODID_LIKE)))
        .addMethod(
          getLikesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesArgs,
              tukano.impl.grpc.generated_java.ShortsProtoBuf.LikesResult>(
                service, METHODID_LIKES)))
        .addMethod(
          getGetFeedMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedArgs,
              tukano.impl.grpc.generated_java.ShortsProtoBuf.GetFeedResult>(
                service, METHODID_GET_FEED)))
        .addMethod(
          getDeleteAllShortsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsArgs,
              tukano.impl.grpc.generated_java.ShortsProtoBuf.DeleteAllShortsResult>(
                service, METHODID_DELETE_ALL_SHORTS)))
        .build();
  }

  private static abstract class ShortsBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ShortsBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return tukano.impl.grpc.generated_java.ShortsProtoBuf.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Shorts");
    }
  }

  private static final class ShortsFileDescriptorSupplier
      extends ShortsBaseDescriptorSupplier {
    ShortsFileDescriptorSupplier() {}
  }

  private static final class ShortsMethodDescriptorSupplier
      extends ShortsBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    ShortsMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (ShortsGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ShortsFileDescriptorSupplier())
              .addMethod(getCreateShortMethod())
              .addMethod(getDeleteShortMethod())
              .addMethod(getGetShortMethod())
              .addMethod(getGetShortsMethod())
              .addMethod(getFollowMethod())
              .addMethod(getFollowersMethod())
              .addMethod(getLikeMethod())
              .addMethod(getLikesMethod())
              .addMethod(getGetFeedMethod())
              .addMethod(getDeleteAllShortsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
