package io.github.mpiumakkho.branchsales.consumer.processing;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.HeaderNames;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import io.github.mpiumakkho.branchsales.consumer.validation.RejectedMessageException;

@Configuration(proxyBeanMethods = false)
class KafkaConsumerConfig {

	/** Record header on dead-letter records with the {@code RejectReason} name. Documented in contract/README.md. */
	static final String REJECT_REASON_HEADER = "reject-reason";

	/**
	 * Publishes rejected records unchanged (same key and value bytes) to the same
	 * partition number of the dead-letter topic. Spring Kafka adds the original
	 * topic, partition, offset and the exception message as headers.
	 */
	@Bean
	DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, byte[]> template,
			@Value("${branch-sales.kafka.dead-letter-topic}") String deadLetterTopic) {
		var recoverer = new DeadLetterPublishingRecoverer(template,
				(record, ex) -> new TopicPartition(deadLetterTopic, record.partition()));
		// A contract rejection is not a code error; the stack trace adds nothing for whoever reads the dead letters
		recoverer.excludeHeader(HeaderNames.HeadersToAdd.EX_STACKTRACE);
		recoverer.setHeadersFunction((record, ex) -> {
			var headers = new RecordHeaders();
			if (ex instanceof RejectedMessageException rejected) {
				headers.add(REJECT_REASON_HEADER, rejected.reason().name().getBytes(StandardCharsets.UTF_8));
			}
			return headers;
		});
		return recoverer;
	}

	/**
	 * Handles failures other than contract rejections, which the listener
	 * handles itself. These are infrastructure failures (database or Kafka not
	 * reachable), so the record is retried with back-off and no attempt limit:
	 * the partition waits instead of skipping data. Records stay in the topic for
	 * its retention period (14 days).
	 */
	@Bean
	DefaultErrorHandler errorHandler() {
		var backOff = new ExponentialBackOff(1_000, 2.0);
		// Stays well below max.poll.interval.ms (5 minutes), since the consumer thread sleeps between attempts
		backOff.setMaxInterval(60_000);
		return new DefaultErrorHandler(backOff);
	}
}
