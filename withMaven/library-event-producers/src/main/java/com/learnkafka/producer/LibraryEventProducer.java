package com.learnkafka.producer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.convert.DurationStyle;
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
		
		/*
		 * 1. blocking call - get metadata about the kafka cluster
		 * 2. send message happens - Return a CompletableFuture*/
		
		var completebleFuture = kafkaTemplate.send(topic, key, value);
		completebleFuture.whenComplete((sendResult, throwable) -> {
			if(throwable != null) {
				handleFailure(key, value, throwable);
			}else {
				handleSuccess(key, value, sendResult);
			}
		});
	}
	
	public SendResult<Integer, String> sendLibraryEventApproach2(LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {
		var key = libraryEvent.libraryEventId();
		var value = objectMapper.writeValueAsString(libraryEvent);
		
		/*
		 * 1. blocking call - get metadata about the kafka cluster
		 * 2. block and wait until the message is sent to the kafka
		*/
		
		var sendResult = kafkaTemplate.send(topic, key, value)
//				.get();
				.get(3, TimeUnit.SECONDS);
		handleSuccess(key, value, sendResult);
		return sendResult;
			
	}
	
	public CompletableFuture<SendResult<Integer, String>> sendLibraryEventApproach3(LibraryEvent libraryEvent) throws JsonProcessingException {
		var key = libraryEvent.libraryEventId();
		var value = objectMapper.writeValueAsString(libraryEvent);
		var producerRecord = buildProducerRecord(key, value);
		var completableFuture = this.kafkaTemplate.send(producerRecord);
		
		return completableFuture.whenComplete((sendResult, throwable) -> {
			if(throwable != null) {
				handleFailure(key, value, throwable);
			}else {
				handleSuccess(key, value, sendResult);
			}
		});
	}
	
	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
		List<Header> recordHeader = List.of(new RecordHeader("event-source", "scanner".getBytes()));
		return new ProducerRecord<>(topic, null, key, value, recordHeader);
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
		log.info("Message Sent Successfully for the key : {}, and the value: {}, partition is {}", key, value, sendResult.getRecordMetadata().partition());
		
	}

	private void handleFailure(Integer key, String value, Throwable throwable) {
		log.error("Error sending the message and the exception is {}", throwable.getMessage(), throwable);
	}
	
}
