package grpc.server;

import com.assignment.grpc.EduCostStatQueryFiveServiceGrpc.EduCostStatQueryFiveServiceImplBase;

import com.mongodb.client.FindIterable;

import io.grpc.stub.StreamObserver;

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.assignment.grpc.*;

import org.example.mongodb.*;

public class QueryFiveService extends EduCostStatQueryFiveServiceGrpc.EduCostStatQueryFiveServiceImplBase{
	
	@Override
	public void queryFive(QueryFiveRequest request,StreamObserver<QueryFiveResponse> responseObserver) {
		System.out.println("Query Five Started");
		int year = request.getYear();
		String type = request.getType();
		String length = request.getLength();
		QueryFiveResponse.Builder response = QueryFiveResponse.newBuilder();
		
		QueryFiveDAO dao = new QueryFiveDAO();
		
		List<Document> docs;
		try {
			docs = dao.query_five(year,length,type);
			for(Document doc:docs)
			{
				String region = doc.getString("_id");
				int totalValue = doc.getInteger("total_value", 1);
				QueryFiveResult result = QueryFiveResult.newBuilder()
						.setId(region)
						.setTotalValue(totalValue)
						.build();
				response.addResults(result);
				responseObserver.onNext(response.build());
				responseObserver.onCompleted();
			}
			}
			
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}
	
	

}
