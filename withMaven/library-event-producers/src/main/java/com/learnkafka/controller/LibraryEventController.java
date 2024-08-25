package com.learnkafka.controller;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LibraryEventController {
	
	private static final Logger log = LoggerFactory.getLogger(LibraryEventController.class);
	
	private final LibraryEventProducer libraryEventProducer;
	
	public LibraryEventController(LibraryEventProducer libraryEventProducer) {
		super();
		this.libraryEventProducer = libraryEventProducer;
	}



	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
		log.info("library event :: {}", libraryEvent);
		this.libraryEventProducer.sendLibraryEvent(libraryEvent);
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}
	
	@PostMapping("/v1/libraryeventByApproach2")
	public ResponseEntity<LibraryEvent> postLibraryEventByApproach2(@RequestBody @Valid LibraryEvent libraryEvent) throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException {
		log.info("library event Approach 2 :: {}", libraryEvent);
		this.libraryEventProducer.sendLibraryEventApproach2(libraryEvent);
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}
	
	@PostMapping("/v1/libraryeventByApproach3")
	public ResponseEntity<LibraryEvent> postLibraryEventByApproach3(@RequestBody @Valid LibraryEvent libraryEvent) throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException {
		log.info("library event Approach 3 :: {}", libraryEvent);
		this.libraryEventProducer.sendLibraryEventApproach3(libraryEvent);
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}
	
	@PutMapping("/v1/libraryevent")
	public ResponseEntity<?> updateLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
		log.info("library event :: {}", libraryEvent);
		
		ResponseEntity<String> BAD_REQUEST = validate(libraryEvent);
		
		if(BAD_REQUEST != null) return BAD_REQUEST;	
		
		this.libraryEventProducer.sendLibraryEvent(libraryEvent);
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}



	private ResponseEntity<String> validate(LibraryEvent libraryEvent) {
		if(libraryEvent.libraryEventId() == null) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
		}
		
		if(!libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE))
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
		
		return null;
	}
}
