package com.assignment.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.54.0)",
    comments = "Source: query.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class EduCostStatQueryOneServiceGrpc {

  private EduCostStatQueryOneServiceGrpc() {}

  public static final String SERVICE_NAME = "EduCostStatQueryOneService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.assignment.grpc.QueryOneRequest,
      com.assignment.grpc.QueryOneResponse> getQueryOneMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryOne",
      requestType = com.assignment.grpc.QueryOneRequest.class,
      responseType = com.assignment.grpc.QueryOneResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.assignment.grpc.QueryOneRequest,
      com.assignment.grpc.QueryOneResponse> getQueryOneMethod() {
    io.grpc.MethodDescriptor<com.assignment.grpc.QueryOneRequest, com.assignment.grpc.QueryOneResponse> getQueryOneMethod;
    if ((getQueryOneMethod = EduCostStatQueryOneServiceGrpc.getQueryOneMethod) == null) {
      synchronized (EduCostStatQueryOneServiceGrpc.class) {
        if ((getQueryOneMethod = EduCostStatQueryOneServiceGrpc.getQueryOneMethod) == null) {
          EduCostStatQueryOneServiceGrpc.getQueryOneMethod = getQueryOneMethod =
              io.grpc.MethodDescriptor.<com.assignment.grpc.QueryOneRequest, com.assignment.grpc.QueryOneResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryOne"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.assignment.grpc.QueryOneRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.assignment.grpc.QueryOneResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EduCostStatQueryOneServiceMethodDescriptorSupplier("QueryOne"))
              .build();
        }
      }
    }
    return getQueryOneMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static EduCostStatQueryOneServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryOneServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryOneServiceStub>() {
        @java.lang.Override
        public EduCostStatQueryOneServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryOneServiceStub(channel, callOptions);
        }
      };
    return EduCostStatQueryOneServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static EduCostStatQueryOneServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryOneServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryOneServiceBlockingStub>() {
        @java.lang.Override
        public EduCostStatQueryOneServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryOneServiceBlockingStub(channel, callOptions);
        }
      };
    return EduCostStatQueryOneServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static EduCostStatQueryOneServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryOneServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryOneServiceFutureStub>() {
        @java.lang.Override
        public EduCostStatQueryOneServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryOneServiceFutureStub(channel, callOptions);
        }
      };
    return EduCostStatQueryOneServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void queryOne(com.assignment.grpc.QueryOneRequest request,
        io.grpc.stub.StreamObserver<com.assignment.grpc.QueryOneResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryOneMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service EduCostStatQueryOneService.
   */
  public static abstract class EduCostStatQueryOneServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return EduCostStatQueryOneServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service EduCostStatQueryOneService.
   */
  public static final class EduCostStatQueryOneServiceStub
      extends io.grpc.stub.AbstractAsyncStub<EduCostStatQueryOneServiceStub> {
    private EduCostStatQueryOneServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryOneServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryOneServiceStub(channel, callOptions);
    }

    /**
     */
    public void queryOne(com.assignment.grpc.QueryOneRequest request,
        io.grpc.stub.StreamObserver<com.assignment.grpc.QueryOneResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryOneMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service EduCostStatQueryOneService.
   */
  public static final class EduCostStatQueryOneServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<EduCostStatQueryOneServiceBlockingStub> {
    private EduCostStatQueryOneServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryOneServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryOneServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.assignment.grpc.QueryOneResponse queryOne(com.assignment.grpc.QueryOneRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryOneMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service EduCostStatQueryOneService.
   */
  public static final class EduCostStatQueryOneServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<EduCostStatQueryOneServiceFutureStub> {
    private EduCostStatQueryOneServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryOneServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryOneServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.assignment.grpc.QueryOneResponse> queryOne(
        com.assignment.grpc.QueryOneRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryOneMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_QUERY_ONE = 0;

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
        case METHODID_QUERY_ONE:
          serviceImpl.queryOne((com.assignment.grpc.QueryOneRequest) request,
              (io.grpc.stub.StreamObserver<com.assignment.grpc.QueryOneResponse>) responseObserver);
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
          getQueryOneMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.assignment.grpc.QueryOneRequest,
              com.assignment.grpc.QueryOneResponse>(
                service, METHODID_QUERY_ONE)))
        .build();
  }

  private static abstract class EduCostStatQueryOneServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    EduCostStatQueryOneServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.assignment.grpc.Query.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("EduCostStatQueryOneService");
    }
  }

  private static final class EduCostStatQueryOneServiceFileDescriptorSupplier
      extends EduCostStatQueryOneServiceBaseDescriptorSupplier {
    EduCostStatQueryOneServiceFileDescriptorSupplier() {}
  }

  private static final class EduCostStatQueryOneServiceMethodDescriptorSupplier
      extends EduCostStatQueryOneServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    EduCostStatQueryOneServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (EduCostStatQueryOneServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new EduCostStatQueryOneServiceFileDescriptorSupplier())
              .addMethod(getQueryOneMethod())
              .build();
        }
      }
    }
    return result;
  }
}
