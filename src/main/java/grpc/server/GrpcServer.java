package grpc.server;

import java.io.IOException;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class GrpcServer {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		int port = 9090;
		
		Server server = ServerBuilder.forPort(port)
				.addService(new QueryOneService())
				.addService(new QueryTwoService())
				.addService(new QueryThreeService())
				.build();
		
		server.start();
		System.out.println("Server Started");
		System.out.println("Listening on port: "+port);
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("Received Shutdown Request");
			server.shutdown();
			System.out.println("Server Stopped");
		}));
		
		server.awaitTermination();
	}

}
