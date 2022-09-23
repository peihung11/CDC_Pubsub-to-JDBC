package com.sync.test.sync_test;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

@SpringBootApplication
public class SyncTestApplication {

	public static void main(String[] args) throws IOException {
		
		String projectId = "das-ct-lab";
		String subscriptionId = "student-topic-test-sub";
		Integer numOfMessages = 10;
	
		subscribeSyncExample(projectId, subscriptionId, numOfMessages);
	}

	public static void subscribeSyncExample(
		String projectId, String subscriptionId, Integer numOfMessages) throws IOException {
	  SubscriberStubSettings subscriberStubSettings =
		  SubscriberStubSettings.newBuilder()
			  .setTransportChannelProvider(
				  SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
					  .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
					  .build())
			  .build();
  
	  try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
		String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
		PullRequest pullRequest =
			PullRequest.newBuilder()
				.setMaxMessages(numOfMessages)
				.setSubscription(subscriptionName)
				.build();
  
		// Use pullCallable().futureCall to asynchronously perform this operation.
		PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
  
		// Stop the program if the pull response is empty to avoid acknowledging
		// an empty list of ack IDs.
		if (pullResponse.getReceivedMessagesList().isEmpty()) {
		  System.out.println("No message was pulled. Exiting.");
		  return;
		}
  
		// ArrayList<String> typelList = new ArrayList<>();
		Map<String, String> type_field_Map = new LinkedHashMap<String, String>();

        
		List<String> ackIds = new ArrayList<>();
		for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {

		  	// Handle received message
		  	// ...
			
			System.out.println("---------------data--------------");
			// System.out.println(message.getClass().toString());
			String data = message.getMessage().getData().toStringUtf8();
			// System.out.println(data);
			
			//解析schema中的欄位type
			System.out.println("---------------JSON data--------------");
			JSONObject jsonObject = new JSONObject(data);
			System.out.println(jsonObject);	
			// System.out.println("---------------schema--------------");						
            String filed_info = jsonObject.get("schema").toString();
			JSONObject payload = (JSONObject) jsonObject.get("payload");
			//取得field的key欄位name
			JSONArray field_key_name = (JSONArray) jsonObject.get("keyField");
			System.out.println(field_key_name);

			// System.out.println("---------------schema2--------------");
			JSONObject jsonObject2 = new JSONObject(filed_info);					
            String filed_info2 = jsonObject2.get("fields").toString();
			// System.out.println(filed_info2);

			// System.out.println("---------------array--------------");	
			JSONArray jsonArray = new JSONArray(filed_info2);
			String filed_info3 = jsonArray.get(1).toString();
			// System.out.println(filed_info3);

			// System.out.println("---------------fields--------------");
			JSONObject jsonObject3 = new JSONObject(filed_info3);					
            String filed_info4 = jsonObject3.get("fields").toString();
			// System.out.println(filed_info4);

			// 將type和field,map起來
			System.out.println("---------------type--------------");
			JSONArray jsonArray2 = new JSONArray(filed_info4);
			for(int i = 0; i < jsonArray2.length(); i++){
				JSONObject jsonObject4 = jsonArray2.getJSONObject(i);		
				type_field_Map.put(jsonObject4.getString("field"),jsonObject4.getString("type"));
				
				
				// //添加type元素
				// typelList.add(jsonObject4.getString("type"));
				// System.out.println("type:"+jsonObject4.getString("type"));
			} 
			System.out.println("---------------將type和field,map起來 (type_field_Map)--------------");
			System.out.println(type_field_Map);
			// String filed_info3 = jsonArray2.get(1).toString();
			// System.out.println(filed_info3);
			// System.out.println(typelList);
			
			

			processDataExample(payload,type_field_Map,field_key_name);

			
			// 嘗試
			// String data = message.getMessage().getData().toStringUtf8();
			// processDataExample(data);
		
		  	ackIds.add(message.getAckId());
		}
  
		// Acknowledge received messages.
		AcknowledgeRequest acknowledgeRequest =
			AcknowledgeRequest.newBuilder()
				.setSubscription(subscriptionName)
				.addAllAckIds(ackIds)
				.build();
  
		// Use acknowledgeCallable().futureCall to asynchronously perform this operation.
		subscriber.acknowledgeCallable().call(acknowledgeRequest);
		// System.out.println(pullResponse.getReceivedMessagesList());

	  }
	}

	//處理data,判斷c,u,d
	public static void processDataExample(JSONObject jsonObject, Map<String, String> type_field_Map,JSONArray field_key_name) {
		System.out.println("------------------到processDataExample data---------------------");
		
		
		//變成json取值		
		ConvertSQL convertSQL = new ConvertSQL();
		if (jsonObject.get("op") !=null){
			String op = jsonObject.get("op").toString();
			//取得 table name
			JSONObject table_name = jsonObject.getJSONObject("source");			
			String table = table_name.get("table").toString();

			//判斷異動的operation
			switch(op){
				case "c":
				JSONObject AfterData = (JSONObject) jsonObject.get("after");
				System.out.println(AfterData);
				System.out.println("is create");
				// JSONObject cAfterData = jsonObject.getJSONObject("after");
				// System.out.println(cAfterData);
				convertSQL.insert(AfterData,type_field_Map,table);
				break;

				case "u":
				JSONObject AfterData2 = (JSONObject) jsonObject.get("after");
				System.out.println(AfterData2);
				System.out.println("is update");
				//update方法
				// JSONObject uAfterData = jsonObject.getJSONObject("after");
				// System.out.println(uAfterData);
				convertSQL.update(AfterData2,type_field_Map,table,field_key_name);				
				break;

				case "d":
				System.out.println("is detele");				
				// System.out.println(deleteId);
				JSONObject BeforeData = (JSONObject) jsonObject.get("before");
				System.out.println(BeforeData);			
				convertSQL.delete(BeforeData,type_field_Map,table,field_key_name);
				break;

				default:
            	System.out.println("other op");
			}

		}
				

	}






}
