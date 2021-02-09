package org.jastdev.demo.pubsublite;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.cloudpubsub.Publisher;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.cloud.pubsublite.cloudpubsub.Subscriber;
import com.google.cloud.pubsublite.cloudpubsub.SubscriberSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

public class Runner {

	public static void main(String[] args) throws Exception {
		String region = args[0].replaceAll("--region=", "");
		String zone = args[1].replaceAll("--zone=", "");
		long project = Long.parseLong(args[2].replaceAll("--projectNumber=", ""));
		String topic = args[3].replaceAll("--topic=", "");
		String subscription = args[4].replaceAll("--subscription=", "");
		String message = args[5].replaceAll("--message=", "");

		CloudRegion cloudRegion = CloudRegion.of(region);
		CloudZone cloudZone = CloudZone.of(cloudRegion, zone.charAt(0));
		ProjectNumber projectNumber = ProjectNumber.of(project);
		TopicName topicName = TopicName.of(topic);
		SubscriptionName subscriptionName = SubscriptionName.of(subscription);
		
		TopicPath topicPath = TopicPath.newBuilder()
				.setLocation(cloudZone)
				.setProject(projectNumber)
				.setName(topicName)
				.build();
		
		SubscriptionPath subscriptionPath = SubscriptionPath.newBuilder()
				.setLocation(cloudZone)
				.setProject(projectNumber)
				.setName(subscriptionName)
				.build();
		
		sendMessage(topicPath, message);
		
		Thread.sleep(2000);
		
		listenToSuscription(subscriptionPath);
	}
	
	private static void sendMessage(TopicPath topicPath, String message) throws ApiException, ExecutionException, InterruptedException {
		PublisherSettings publisherSettings = PublisherSettings.newBuilder()
				.setTopicPath(topicPath)
				.build();
		
		List<ApiFuture<String>> futures = new ArrayList<>();
		Publisher publisher = null;
		try {
			 publisher = Publisher.create(publisherSettings);
			 
			 publisher.startAsync().awaitRunning();
			 
			 PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
					 .setData(ByteString.copyFromUtf8(message))
					 .build();
			 
			 ApiFuture<String> future = publisher.publish(pubsubMessage);
			 futures.add(future);
		} finally {
			List<String> ackIds = ApiFutures.allAsList(futures).get();
			System.out.println("Published " + ackIds.size() + " messages");
			
			if (publisher != null) publisher.stopAsync().awaitTerminated();
		}
	}
	
	private static void listenToSuscription(SubscriptionPath subscriptionPath) {
		FlowControlSettings flowControlSettings = FlowControlSettings.builder()
				.setBytesOutstanding(10 * 1024 * 1024)
				.setMessagesOutstanding(1000)
				.build();
		
		SubscriberSettings subscriberSettings = SubscriberSettings.newBuilder()
				.setSubscriptionPath(subscriptionPath)
				.setReceiver(new SimpleMessageReceiver())
				.setPerPartitionFlowControlSettings(flowControlSettings)
				.build();
		
		Subscriber subscriber = Subscriber.create(subscriberSettings);
		subscriber.startAsync().awaitRunning();
		System.out.println("Listening to messages on " + subscriptionPath.toString());

		try {
			System.out.println(subscriber.state());
			subscriber.awaitTerminated(90, TimeUnit.SECONDS);
		} catch (TimeoutException ex) {
			System.err.println("Error: " + ex.getMessage());
			subscriber.stopAsync().awaitTerminated();
		}
	}
	
}
