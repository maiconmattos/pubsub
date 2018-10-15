package lab.gcp.pubsub;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

public class PublisherMain {

	public static void main(String[] args) throws Exception {

		String projectId = "gcplearning-219114";
		String topicId = "poc";
		
		ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
		Publisher publisher = null;
		List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

		try {
			// Create a publisher instance with default settings bound to the topic
			publisher = Publisher.newBuilder(topicName).build();

			List<String> messages = Arrays.asList("first message! Timestamp : " + new Date().getTime(),
					"second message! Timestamp :" + new Date().getTime());

			// schedule publishing one message at a time : messages get automatically
			// batched
			for (String message : messages) {
				ByteString data = ByteString.copyFromUtf8(message);
				PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

				// Once published, returns a server-assigned message id (unique within the
				// topic)
				ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
				messageIdFutures.add(messageIdFuture);
			}
		} finally {
			// wait on any pending publish requests.
			List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

			for (String messageId : messageIds) {
				System.out.println("published with message ID: " + messageId);
			}

			if (publisher != null) {
				// When finished with the publisher, shutdown to free up resources.
				publisher.shutdown();
			}
		}
	}

}
