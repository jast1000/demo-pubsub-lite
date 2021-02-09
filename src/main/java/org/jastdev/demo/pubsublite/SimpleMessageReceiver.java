package org.jastdev.demo.pubsublite;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;


public class SimpleMessageReceiver implements MessageReceiver {

	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		System.out.println("Mensaje: " + message.getData().toStringUtf8());
		
		consumer.ack();
	}

}
