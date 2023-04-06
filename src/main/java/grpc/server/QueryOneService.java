package grpc.server;

import com.assignment.grpc.EduCostStatQueryOneServiceGrpc.EduCostStatQueryOneServiceImplBase;
import com.mongodb.client.FindIterable;

import io.grpc.stub.StreamObserver;

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.assignment.grpc.*;

import org.example.mongodb.*;

public class QueryOneService extends EduCostStatQueryOneServiceGrpc.EduCostStatQueryOneServiceImplBase{
	
	@Override
	public void queryOne(QueryOneRequest request,StreamObserver<QueryOneResponse> responseObserver) {
		System.out.println("Query One Started");
		int Year = request.getYear();
		String State = request.getState();
		String Type = request.getType();
		String Length = request.getLength();
		String Expense = request.getExpense();
		QueryOneResponse.Builder response = QueryOneResponse.newBuilder();
		
		QueryOneDAO dao = new QueryOneDAO();
	
		
			List<Document> docs;
			try {
				docs = dao.query_one(Year, State, Type, Length, Expense);
				for(Document doc:docs)
				{
					response.addValue(doc.getInteger("Value"));
				}
				System.out.println(docs);
				responseObserver.onNext(response.build());
				responseObserver.onCompleted();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		
	}

}
