package grpc.server;

import com.assignment.grpc.EduCostStatQueryFourServiceGrpc.EduCostStatQueryFourServiceImplBase;

import com.mongodb.client.FindIterable;

import io.grpc.stub.StreamObserver;

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.assignment.grpc.*;

import org.example.mongodb.*;

public class QueryFourService extends EduCostStatQueryFourServiceGrpc.EduCostStatQueryFourServiceImplBase{
																			
	@Override
	public void queryFour(QueryFourRequest request,StreamObserver<QueryFourResponse> responseObserver) {
		System.out.println("Query Four Started");
		String Type = request.getType();
		String Length = request.getLength();
		int range = request.getRange();
		QueryFourResponse.Builder response = QueryFourResponse.newBuilder();
		
		QueryFourDAO dao = new QueryFourDAO();
		
		List<Document> docs;
		try {
			docs = dao.query_four(Type, Length, range);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
