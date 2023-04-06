package com.assignment.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.54.0)",
    comments = "Source: query.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class EduCostStatQueryTwoServiceGrpc {

  private EduCostStatQueryTwoServiceGrpc() {}

  public static final String SERVICE_NAME = "EduCostStatQueryTwoService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.assignment.grpc.QueryTwoRequest,
      com.assignment.grpc.QueryTwoResponse> getQueryTwoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryTwo",
      requestType = com.assignment.grpc.QueryTwoRequest.class,
      responseType = com.assignment.grpc.QueryTwoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.assignment.grpc.QueryTwoRequest,
      com.assignment.grpc.QueryTwoResponse> getQueryTwoMethod() {
    io.grpc.MethodDescriptor<com.assignment.grpc.QueryTwoRequest, com.assignment.grpc.QueryTwoResponse> getQueryTwoMethod;
    if ((getQueryTwoMethod = EduCostStatQueryTwoServiceGrpc.getQueryTwoMethod) == null) {
      synchronized (EduCostStatQueryTwoServiceGrpc.class) {
        if ((getQueryTwoMethod = EduCostStatQueryTwoServiceGrpc.getQueryTwoMethod) == null) {
          EduCostStatQueryTwoServiceGrpc.getQueryTwoMethod = getQueryTwoMethod =
              io.grpc.MethodDescriptor.<com.assignment.grpc.QueryTwoRequest, com.assignment.grpc.QueryTwoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryTwo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.assignment.grpc.QueryTwoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.assignment.grpc.QueryTwoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EduCostStatQueryTwoServiceMethodDescriptorSupplier("QueryTwo"))
              .build();
        }
      }
    }
    return getQueryTwoMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static EduCostStatQueryTwoServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryTwoServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryTwoServiceStub>() {
        @java.lang.Override
        public EduCostStatQueryTwoServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryTwoServiceStub(channel, callOptions);
        }
      };
    return EduCostStatQueryTwoServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static EduCostStatQueryTwoServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryTwoServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryTwoServiceBlockingStub>() {
        @java.lang.Override
        public EduCostStatQueryTwoServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryTwoServiceBlockingStub(channel, callOptions);
        }
      };
    return EduCostStatQueryTwoServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static EduCostStatQueryTwoServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryTwoServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryTwoServiceFutureStub>() {
        @java.lang.Override
        public EduCostStatQueryTwoServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryTwoServiceFutureStub(channel, callOptions);
        }
      };
    return EduCostStatQueryTwoServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void queryTwo(com.assignment.grpc.QueryTwoRequest request,
        io.grpc.stub.StreamObserver<com.assignment.grpc.QueryTwoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryTwoMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service EduCostStatQueryTwoService.
   */
  public static abstract class EduCostStatQueryTwoServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return EduCostStatQueryTwoServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service EduCostStatQueryTwoService.
   */
  public static final class EduCostStatQueryTwoServiceStub
      extends io.grpc.stub.AbstractAsyncStub<EduCostStatQueryTwoServiceStub> {
    private EduCostStatQueryTwoServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryTwoServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryTwoServiceStub(channel, callOptions);
    }

    /**
     */
    public void queryTwo(com.assignment.grpc.QueryTwoRequest request,
        io.grpc.stub.StreamObserver<com.assignment.grpc.QueryTwoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryTwoMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service EduCostStatQueryTwoService.
   */
  public static final class EduCostStatQueryTwoServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<EduCostStatQueryTwoServiceBlockingStub> {
    private EduCostStatQueryTwoServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryTwoServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryTwoServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.assignment.grpc.QueryTwoResponse queryTwo(com.assignment.grpc.QueryTwoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryTwoMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service EduCostStatQueryTwoService.
   */
  public static final class EduCostStatQueryTwoServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<EduCostStatQueryTwoServiceFutureStub> {
    private EduCostStatQueryTwoServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryTwoServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryTwoServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.assignment.grpc.QueryTwoResponse> queryTwo(
        com.assignment.grpc.QueryTwoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryTwoMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_QUERY_TWO = 0;

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
        case METHODID_QUERY_TWO:
          serviceImpl.queryTwo((com.assignment.grpc.QueryTwoRequest) request,
              (io.grpc.stub.StreamObserver<com.assignment.grpc.QueryTwoResponse>) responseObserver);
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
          getQueryTwoMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.assignment.grpc.QueryTwoRequest,
              com.assignment.grpc.QueryTwoResponse>(
                service, METHODID_QUERY_TWO)))
        .build();
  }

  private static abstract class EduCostStatQueryTwoServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    EduCostStatQueryTwoServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.assignment.grpc.Query.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("EduCostStatQueryTwoService");
    }
  }

  private static final class EduCostStatQueryTwoServiceFileDescriptorSupplier
      extends EduCostStatQueryTwoServiceBaseDescriptorSupplier {
    EduCostStatQueryTwoServiceFileDescriptorSupplier() {}
  }

  private static final class EduCostStatQueryTwoServiceMethodDescriptorSupplier
      extends EduCostStatQueryTwoServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    EduCostStatQueryTwoServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (EduCostStatQueryTwoServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new EduCostStatQueryTwoServiceFileDescriptorSupplier())
              .addMethod(getQueryTwoMethod())
              .build();
        }
      }
    }
    return result;
  }
}
