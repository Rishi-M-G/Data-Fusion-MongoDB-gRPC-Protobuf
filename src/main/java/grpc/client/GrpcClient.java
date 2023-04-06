package grpc.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.assignment.grpc.*;
import com.assignment.grpc.EduCostStatQueryOneServiceGrpc;
import com.assignment.grpc.EduCostStatQueryTwoServiceGrpc;
import com.assignment.grpc.EduCostStatQueryThreeServiceGrpc;
import com.assignment.grpc.EduCostStatQueryFourServiceGrpc;
import com.assignment.grpc.EduCostStatQueryFiveServiceGrpc;

import com.assignment.grpc.EduCostStatQueryOneServiceGrpc.EduCostStatQueryOneServiceBlockingStub;
import com.assignment.grpc.EduCostStatQueryTwoServiceGrpc.EduCostStatQueryTwoServiceBlockingStub;
import com.assignment.grpc.EduCostStatQueryThreeServiceGrpc.EduCostStatQueryThreeServiceBlockingStub;
import com.assignment.grpc.EduCostStatQueryFourServiceGrpc.EduCostStatQueryFourServiceBlockingStub;
import com.assignment.grpc.EduCostStatQueryFiveServiceGrpc.EduCostStatQueryFiveServiceBlockingStub;

public class GrpcClient {

	public static void main(String[] args) throws InterruptedException {
		ManagedChannel channel = ManagedChannelBuilder
				.forAddress("localhost",9090)
				.usePlaintext()
				.build();
		
		EduCostStatQueryOneServiceBlockingStub query1stub = EduCostStatQueryOneServiceGrpc.newBlockingStub(channel);
		EduCostStatQueryTwoServiceBlockingStub query2stub = EduCostStatQueryTwoServiceGrpc.newBlockingStub(channel);
		EduCostStatQueryThreeServiceBlockingStub query3stub = EduCostStatQueryThreeServiceGrpc.newBlockingStub(channel);
		EduCostStatQueryFourServiceBlockingStub query4stub = EduCostStatQueryFourServiceGrpc.newBlockingStub(channel);
		EduCostStatQueryFiveServiceBlockingStub query5stub = EduCostStatQueryFiveServiceGrpc.newBlockingStub(channel);
		
		
		
		
		QueryOneRequest query1 = QueryOneRequest.newBuilder().setYear(2013).setState("Alabama").setType("Private").setLength("4-year").setExpense("Fees/Tuition").build();
		QueryOneResponse response1 = query1stub.queryOne(query1);
		System.out.println("Query 1 Executed");
		List op1 = new ArrayList();
		op1 = response1.getValueList();
		System.out.println("Value : "+op1.get(0));
		
		System.out.println(" ******************** ");
		System.out.println();
		
		QueryTwoRequest query2 = QueryTwoRequest.newBuilder().setYear(2013).setType("Private").setLength("4-year").build();
		QueryTwoResponse response2 = query2stub.queryTwo(query2);
		System.out.println("Query 2 Executed");
		List op2 = new ArrayList();
		op2 = response2.getStateExpenseList();
		System.out.println("Output : "+ op2);
		
		System.out.println(" ******************** ");
		System.out.println();
		
		QueryThreeRequest query3 = QueryThreeRequest.newBuilder().setYear(2013).setType("Private").setLength("4-year").build();
		QueryThreeResponse response3 = query3stub.queryThree(query3);
		System.out.println("Query 3 Executed");
		List op3 = new ArrayList();
		op3 = response3.getStateExpenseList();
		System.out.println("Output : "+ op3);
		
		System.out.println(" ******************** ");
		System.out.println();
		
		Thread.sleep(1000);
		
//		QueryFourRequest query4 = QueryFourRequest.newBuilder().setType("Private").setLength("4-year").setRange(3).build();
//		QueryFourResponse response4 = query4stub.queryFour(query4);
//		System.out.println("Query 4 Executed");
//		List op4 = new ArrayList();
//		op4 = response4.getResultsList();
//		System.out.println("Output : "+ op4);
//
//		QueryFiveRequest query5 = QueryFiveRequest.newBuilder().setYear(2013).setLength("4-year").setType("Private").build();
//		QueryFiveResponse response5 = query5stub.queryFive(query5);
//		System.out.println("Query 5 Executed");
//		List op5 = new ArrayList();
//		op5 = response5.getResultsList();
//		System.out.println("Output : "+ op5);

		System.out.println("Shutting down");
		channel.shutdown();
	}

}
