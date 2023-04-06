package grpc.server;

import com.assignment.grpc.EduCostStatQueryThreeServiceGrpc.EduCostStatQueryThreeServiceImplBase;

import com.mongodb.client.FindIterable;

import io.grpc.stub.StreamObserver;

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.assignment.grpc.*;

import org.example.mongodb.*;


public class QueryThreeService extends EduCostStatQueryThreeServiceGrpc.EduCostStatQueryThreeServiceImplBase{
	
	@Override
	public void queryThree(QueryThreeRequest request,StreamObserver<QueryThreeResponse> responseObserver) {
		System.out.println("Query Three Started");
		int Year = request.getYear();
		String Type = request.getType();
		String Length = request.getLength();
		QueryThreeResponse.Builder response = QueryThreeResponse.newBuilder();
		 
		QueryThreeDAO dao = new QueryThreeDAO();
		
		List<Document> docs;
		try {
			docs = dao.query_three(Year, Type, Length);
			for(Document doc:docs)
			{
				String state = doc.getString("_id");
				int overallExpense = doc.getInteger("Overall Expense", 1);
				StateExpenseQueryThree stateExpense = StateExpenseQueryThree.newBuilder()
						.setState(state)
						.setOverallExpense(overallExpense)
						.build();
				response.addStateExpense(stateExpense);
			}
			responseObserver.onNext(response.build());
			responseObserver.onCompleted();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
