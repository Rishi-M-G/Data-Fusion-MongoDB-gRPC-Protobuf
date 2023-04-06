package com.assignment.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.54.0)",
    comments = "Source: query.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class EduCostStatQueryFourServiceGrpc {

  private EduCostStatQueryFourServiceGrpc() {}

  public static final String SERVICE_NAME = "EduCostStatQueryFourService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.assignment.grpc.QueryFourRequest,
      com.assignment.grpc.QueryFourResponse> getQueryFourMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryFour",
      requestType = com.assignment.grpc.QueryFourRequest.class,
      responseType = com.assignment.grpc.QueryFourResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.assignment.grpc.QueryFourRequest,
      com.assignment.grpc.QueryFourResponse> getQueryFourMethod() {
    io.grpc.MethodDescriptor<com.assignment.grpc.QueryFourRequest, com.assignment.grpc.QueryFourResponse> getQueryFourMethod;
    if ((getQueryFourMethod = EduCostStatQueryFourServiceGrpc.getQueryFourMethod) == null) {
      synchronized (EduCostStatQueryFourServiceGrpc.class) {
        if ((getQueryFourMethod = EduCostStatQueryFourServiceGrpc.getQueryFourMethod) == null) {
          EduCostStatQueryFourServiceGrpc.getQueryFourMethod = getQueryFourMethod =
              io.grpc.MethodDescriptor.<com.assignment.grpc.QueryFourRequest, com.assignment.grpc.QueryFourResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryFour"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.assignment.grpc.QueryFourRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.assignment.grpc.QueryFourResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EduCostStatQueryFourServiceMethodDescriptorSupplier("QueryFour"))
              .build();
        }
      }
    }
    return getQueryFourMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static EduCostStatQueryFourServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFourServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFourServiceStub>() {
        @java.lang.Override
        public EduCostStatQueryFourServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryFourServiceStub(channel, callOptions);
        }
      };
    return EduCostStatQueryFourServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static EduCostStatQueryFourServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFourServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFourServiceBlockingStub>() {
        @java.lang.Override
        public EduCostStatQueryFourServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryFourServiceBlockingStub(channel, callOptions);
        }
      };
    return EduCostStatQueryFourServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static EduCostStatQueryFourServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFourServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFourServiceFutureStub>() {
        @java.lang.Override
        public EduCostStatQueryFourServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryFourServiceFutureStub(channel, callOptions);
        }
      };
    return EduCostStatQueryFourServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void queryFour(com.assignment.grpc.QueryFourRequest request,
        io.grpc.stub.StreamObserver<com.assignment.grpc.QueryFourResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryFourMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service EduCostStatQueryFourService.
   */
  public static abstract class EduCostStatQueryFourServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return EduCostStatQueryFourServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service EduCostStatQueryFourService.
   */
  public static final class EduCostStatQueryFourServiceStub
      extends io.grpc.stub.AbstractAsyncStub<EduCostStatQueryFourServiceStub> {
    private EduCostStatQueryFourServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryFourServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryFourServiceStub(channel, callOptions);
    }

    /**
     */
    public void queryFour(com.assignment.grpc.QueryFourRequest request,
        io.grpc.stub.StreamObserver<com.assignment.grpc.QueryFourResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryFourMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service EduCostStatQueryFourService.
   */
  public static final class EduCostStatQueryFourServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<EduCostStatQueryFourServiceBlockingStub> {
    private EduCostStatQueryFourServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryFourServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryFourServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.assignment.grpc.QueryFourResponse queryFour(com.assignment.grpc.QueryFourRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryFourMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service EduCostStatQueryFourService.
   */
  public static final class EduCostStatQueryFourServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<EduCostStatQueryFourServiceFutureStub> {
    private EduCostStatQueryFourServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryFourServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryFourServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.assignment.grpc.QueryFourResponse> queryFour(
        com.assignment.grpc.QueryFourRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryFourMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_QUERY_FOUR = 0;

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
        case METHODID_QUERY_FOUR:
          serviceImpl.queryFour((com.assignment.grpc.QueryFourRequest) request,
              (io.grpc.stub.StreamObserver<com.assignment.grpc.QueryFourResponse>) responseObserver);
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
          getQueryFourMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.assignment.grpc.QueryFourRequest,
              com.assignment.grpc.QueryFourResponse>(
                service, METHODID_QUERY_FOUR)))
        .build();
  }

  private static abstract class EduCostStatQueryFourServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    EduCostStatQueryFourServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.assignment.grpc.Query.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("EduCostStatQueryFourService");
    }
  }

  private static final class EduCostStatQueryFourServiceFileDescriptorSupplier
      extends EduCostStatQueryFourServiceBaseDescriptorSupplier {
    EduCostStatQueryFourServiceFileDescriptorSupplier() {}
  }

  private static final class EduCostStatQueryFourServiceMethodDescriptorSupplier
      extends EduCostStatQueryFourServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    EduCostStatQueryFourServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (EduCostStatQueryFourServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new EduCostStatQueryFourServiceFileDescriptorSupplier())
              .addMethod(getQueryFourMethod())
              .build();
        }
      }
    }
    return result;
  }
}
