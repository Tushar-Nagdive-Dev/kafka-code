package com.learnkafka.controller;

import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class LibraryEventControllerAdvise {
	
	private static final Logger log = LoggerFactory.getLogger(LibraryEventControllerAdvise.class);
	
	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<?> exceptionProcessor(MethodArgumentNotValidException ex) {
		var errorMessage = ex.getBindingResult().getFieldErrors().stream()
		.map(fieldError -> fieldError.getField() + " " +fieldError.getDefaultMessage())
		.sorted().collect(Collectors.joining(", "));
		
		log.info("errorMessage : {}", errorMessage);
		return new ResponseEntity<>(errorMessage, HttpStatus.BAD_REQUEST);
	}
}
