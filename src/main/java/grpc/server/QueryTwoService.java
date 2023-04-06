package grpc.server;

import com.assignment.grpc.EduCostStatQueryTwoServiceGrpc.EduCostStatQueryTwoServiceImplBase;
import com.mongodb.client.FindIterable;

import io.grpc.stub.StreamObserver;

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.assignment.grpc.*;

import org.example.mongodb.*;

public class QueryTwoService extends EduCostStatQueryTwoServiceGrpc.EduCostStatQueryTwoServiceImplBase {
	
	@Override
	public void queryTwo(QueryTwoRequest request,StreamObserver<QueryTwoResponse> responseObserver) {
		
		System.out.println("Query Two Started");
		int Year = request.getYear();
		String Type = request.getType();
		String Length = request.getLength();
		QueryTwoResponse.Builder response = QueryTwoResponse.newBuilder();
		
		QueryTwoDAO dao = new QueryTwoDAO();
		
		List<Document> docs;
		try {
			docs = dao.query_two(Year, Type, Length);
			for(Document doc:docs)
			{
				String state = doc.getString("_id");
				int overallExpense = doc.getInteger("Overall Expense", 1);
				StateExpenseQueryTwo stateExpense = StateExpenseQueryTwo.newBuilder()
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
