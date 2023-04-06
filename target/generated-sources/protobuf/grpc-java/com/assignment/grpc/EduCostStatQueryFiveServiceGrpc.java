package com.assignment.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.54.0)",
    comments = "Source: query.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class EduCostStatQueryFiveServiceGrpc {

  private EduCostStatQueryFiveServiceGrpc() {}

  public static final String SERVICE_NAME = "EduCostStatQueryFiveService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.assignment.grpc.QueryFiveRequest,
      com.assignment.grpc.QueryFiveResponse> getQueryFiveMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryFive",
      requestType = com.assignment.grpc.QueryFiveRequest.class,
      responseType = com.assignment.grpc.QueryFiveResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.assignment.grpc.QueryFiveRequest,
      com.assignment.grpc.QueryFiveResponse> getQueryFiveMethod() {
    io.grpc.MethodDescriptor<com.assignment.grpc.QueryFiveRequest, com.assignment.grpc.QueryFiveResponse> getQueryFiveMethod;
    if ((getQueryFiveMethod = EduCostStatQueryFiveServiceGrpc.getQueryFiveMethod) == null) {
      synchronized (EduCostStatQueryFiveServiceGrpc.class) {
        if ((getQueryFiveMethod = EduCostStatQueryFiveServiceGrpc.getQueryFiveMethod) == null) {
          EduCostStatQueryFiveServiceGrpc.getQueryFiveMethod = getQueryFiveMethod =
              io.grpc.MethodDescriptor.<com.assignment.grpc.QueryFiveRequest, com.assignment.grpc.QueryFiveResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryFive"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.assignment.grpc.QueryFiveRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.assignment.grpc.QueryFiveResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EduCostStatQueryFiveServiceMethodDescriptorSupplier("QueryFive"))
              .build();
        }
      }
    }
    return getQueryFiveMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static EduCostStatQueryFiveServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFiveServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFiveServiceStub>() {
        @java.lang.Override
        public EduCostStatQueryFiveServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryFiveServiceStub(channel, callOptions);
        }
      };
    return EduCostStatQueryFiveServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static EduCostStatQueryFiveServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFiveServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFiveServiceBlockingStub>() {
        @java.lang.Override
        public EduCostStatQueryFiveServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryFiveServiceBlockingStub(channel, callOptions);
        }
      };
    return EduCostStatQueryFiveServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static EduCostStatQueryFiveServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFiveServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryFiveServiceFutureStub>() {
        @java.lang.Override
        public EduCostStatQueryFiveServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryFiveServiceFutureStub(channel, callOptions);
        }
      };
    return EduCostStatQueryFiveServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void queryFive(com.assignment.grpc.QueryFiveRequest request,
        io.grpc.stub.StreamObserver<com.assignment.grpc.QueryFiveResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryFiveMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service EduCostStatQueryFiveService.
   */
  public static abstract class EduCostStatQueryFiveServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return EduCostStatQueryFiveServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service EduCostStatQueryFiveService.
   */
  public static final class EduCostStatQueryFiveServiceStub
      extends io.grpc.stub.AbstractAsyncStub<EduCostStatQueryFiveServiceStub> {
    private EduCostStatQueryFiveServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryFiveServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryFiveServiceStub(channel, callOptions);
    }

    /**
     */
    public void queryFive(com.assignment.grpc.QueryFiveRequest request,
        io.grpc.stub.StreamObserver<com.assignment.grpc.QueryFiveResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryFiveMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service EduCostStatQueryFiveService.
   */
  public static final class EduCostStatQueryFiveServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<EduCostStatQueryFiveServiceBlockingStub> {
    private EduCostStatQueryFiveServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryFiveServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryFiveServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.assignment.grpc.QueryFiveResponse queryFive(com.assignment.grpc.QueryFiveRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryFiveMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service EduCostStatQueryFiveService.
   */
  public static final class EduCostStatQueryFiveServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<EduCostStatQueryFiveServiceFutureStub> {
    private EduCostStatQueryFiveServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryFiveServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryFiveServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.assignment.grpc.QueryFiveResponse> queryFive(
        com.assignment.grpc.QueryFiveRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryFiveMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_QUERY_FIVE = 0;

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
        case METHODID_QUERY_FIVE:
          serviceImpl.queryFive((com.assignment.grpc.QueryFiveRequest) request,
              (io.grpc.stub.StreamObserver<com.assignment.grpc.QueryFiveResponse>) responseObserver);
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
          getQueryFiveMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.assignment.grpc.QueryFiveRequest,
              com.assignment.grpc.QueryFiveResponse>(
                service, METHODID_QUERY_FIVE)))
        .build();
  }

  private static abstract class EduCostStatQueryFiveServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    EduCostStatQueryFiveServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.assignment.grpc.Query.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("EduCostStatQueryFiveService");
    }
  }

  private static final class EduCostStatQueryFiveServiceFileDescriptorSupplier
      extends EduCostStatQueryFiveServiceBaseDescriptorSupplier {
    EduCostStatQueryFiveServiceFileDescriptorSupplier() {}
  }

  private static final class EduCostStatQueryFiveServiceMethodDescriptorSupplier
      extends EduCostStatQueryFiveServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    EduCostStatQueryFiveServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (EduCostStatQueryFiveServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new EduCostStatQueryFiveServiceFileDescriptorSupplier())
              .addMethod(getQueryFiveMethod())
              .build();
        }
      }
    }
    return result;
  }
}
