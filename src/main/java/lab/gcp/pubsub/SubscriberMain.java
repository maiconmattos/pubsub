package lab.gcp.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

public class SubscriberMain {

	public static void main(String[] args) {

		String projectId = "gcplearning-219114";
		String subscriptionId = "poc2";

		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
		// Instantiate an asynchronous message receiver
		MessageReceiver receiver = new MessageReceiver() {
			@Override
			public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
				// handle incoming message, then ack/nack the received message
				System.out.println("Id : " + message.getMessageId());
				System.out.println("Raw Data : " + message.getData());
				System.out.println("String Data : " + message.getData().toStringUtf8());
				consumer.ack();
			}
		};

		Subscriber subscriber = null;
		try {
			// Create a subscriber for "my-subscription-id" bound to the message receiver
			subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
			System.out.println("Starting");
			subscriber.startAsync();
			// ...
		} finally {
//			// stop receiving messages
//			if (subscriber != null) {
//				System.out.println("Stopping");
//				subscriber.stopAsync();
//			}
		}
		System.out.println("Listening");
		while (true)
			;
	}

}
