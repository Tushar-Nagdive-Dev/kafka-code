package com.learnkafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;


@Component
public class LibraryEventProducer {
	
	private static final Logger log = LoggerFactory.getLogger(LibraryEventProducer.class);
	
	@Value("${spring.kafka.topic}")
	public String topic;
	
	private final KafkaTemplate<Integer, String> kafkaTemplate;
	
	private final ObjectMapper objectMapper;
	
	public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}
	
	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		var key = libraryEvent.libraryEventId();
		var value = objectMapper.writeValueAsString(libraryEvent);
		
		var completebleFuture = kafkaTemplate.send(topic, key, value);
		completebleFuture.whenComplete((sendResult, throwable) -> {
			if(throwable != null) {
				handleFailure(key, value, throwable);
			}else {
				handleSuccess(key, value, sendResult);
			}
		});
	}
	
	private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
		log.info("Message Sent Successfully for the key : {}, and the value: {}, partition is {}", key, value, sendResult.getRecordMetadata().partition());
		
	}

	private void handleFailure(Integer key, String value, Throwable throwable) {
		log.error("Error sending the message and the exception is {}", throwable.getMessage(), throwable);
	}
	
}
