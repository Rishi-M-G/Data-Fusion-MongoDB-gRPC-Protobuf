package com.assignment.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.54.0)",
    comments = "Source: query.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class EduCostStatQueryThreeServiceGrpc {

  private EduCostStatQueryThreeServiceGrpc() {}

  public static final String SERVICE_NAME = "EduCostStatQueryThreeService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.assignment.grpc.QueryThreeRequest,
      com.assignment.grpc.QueryThreeResponse> getQueryThreeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryThree",
      requestType = com.assignment.grpc.QueryThreeRequest.class,
      responseType = com.assignment.grpc.QueryThreeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.assignment.grpc.QueryThreeRequest,
      com.assignment.grpc.QueryThreeResponse> getQueryThreeMethod() {
    io.grpc.MethodDescriptor<com.assignment.grpc.QueryThreeRequest, com.assignment.grpc.QueryThreeResponse> getQueryThreeMethod;
    if ((getQueryThreeMethod = EduCostStatQueryThreeServiceGrpc.getQueryThreeMethod) == null) {
      synchronized (EduCostStatQueryThreeServiceGrpc.class) {
        if ((getQueryThreeMethod = EduCostStatQueryThreeServiceGrpc.getQueryThreeMethod) == null) {
          EduCostStatQueryThreeServiceGrpc.getQueryThreeMethod = getQueryThreeMethod =
              io.grpc.MethodDescriptor.<com.assignment.grpc.QueryThreeRequest, com.assignment.grpc.QueryThreeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryThree"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.assignment.grpc.QueryThreeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.assignment.grpc.QueryThreeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EduCostStatQueryThreeServiceMethodDescriptorSupplier("QueryThree"))
              .build();
        }
      }
    }
    return getQueryThreeMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static EduCostStatQueryThreeServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryThreeServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryThreeServiceStub>() {
        @java.lang.Override
        public EduCostStatQueryThreeServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryThreeServiceStub(channel, callOptions);
        }
      };
    return EduCostStatQueryThreeServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static EduCostStatQueryThreeServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryThreeServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryThreeServiceBlockingStub>() {
        @java.lang.Override
        public EduCostStatQueryThreeServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryThreeServiceBlockingStub(channel, callOptions);
        }
      };
    return EduCostStatQueryThreeServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static EduCostStatQueryThreeServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryThreeServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EduCostStatQueryThreeServiceFutureStub>() {
        @java.lang.Override
        public EduCostStatQueryThreeServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EduCostStatQueryThreeServiceFutureStub(channel, callOptions);
        }
      };
    return EduCostStatQueryThreeServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void queryThree(com.assignment.grpc.QueryThreeRequest request,
        io.grpc.stub.StreamObserver<com.assignment.grpc.QueryThreeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryThreeMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service EduCostStatQueryThreeService.
   */
  public static abstract class EduCostStatQueryThreeServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return EduCostStatQueryThreeServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service EduCostStatQueryThreeService.
   */
  public static final class EduCostStatQueryThreeServiceStub
      extends io.grpc.stub.AbstractAsyncStub<EduCostStatQueryThreeServiceStub> {
    private EduCostStatQueryThreeServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryThreeServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryThreeServiceStub(channel, callOptions);
    }

    /**
     */
    public void queryThree(com.assignment.grpc.QueryThreeRequest request,
        io.grpc.stub.StreamObserver<com.assignment.grpc.QueryThreeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryThreeMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service EduCostStatQueryThreeService.
   */
  public static final class EduCostStatQueryThreeServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<EduCostStatQueryThreeServiceBlockingStub> {
    private EduCostStatQueryThreeServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryThreeServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryThreeServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.assignment.grpc.QueryThreeResponse queryThree(com.assignment.grpc.QueryThreeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryThreeMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service EduCostStatQueryThreeService.
   */
  public static final class EduCostStatQueryThreeServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<EduCostStatQueryThreeServiceFutureStub> {
    private EduCostStatQueryThreeServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EduCostStatQueryThreeServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EduCostStatQueryThreeServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.assignment.grpc.QueryThreeResponse> queryThree(
        com.assignment.grpc.QueryThreeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryThreeMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_QUERY_THREE = 0;

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
        case METHODID_QUERY_THREE:
          serviceImpl.queryThree((com.assignment.grpc.QueryThreeRequest) request,
              (io.grpc.stub.StreamObserver<com.assignment.grpc.QueryThreeResponse>) responseObserver);
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
          getQueryThreeMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.assignment.grpc.QueryThreeRequest,
              com.assignment.grpc.QueryThreeResponse>(
                service, METHODID_QUERY_THREE)))
        .build();
  }

  private static abstract class EduCostStatQueryThreeServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    EduCostStatQueryThreeServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.assignment.grpc.Query.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("EduCostStatQueryThreeService");
    }
  }

  private static final class EduCostStatQueryThreeServiceFileDescriptorSupplier
      extends EduCostStatQueryThreeServiceBaseDescriptorSupplier {
    EduCostStatQueryThreeServiceFileDescriptorSupplier() {}
  }

  private static final class EduCostStatQueryThreeServiceMethodDescriptorSupplier
      extends EduCostStatQueryThreeServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    EduCostStatQueryThreeServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (EduCostStatQueryThreeServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new EduCostStatQueryThreeServiceFileDescriptorSupplier())
              .addMethod(getQueryThreeMethod())
              .build();
        }
      }
    }
    return result;
  }
}
