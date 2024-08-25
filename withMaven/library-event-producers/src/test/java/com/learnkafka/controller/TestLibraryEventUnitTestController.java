package com.learnkafka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import com.learnkafka.utils.TestUtil;


//Test slice

@WebMvcTest(LibraryEventController.class)
class TestLibraryEventUnitTestController {

	@Autowired
	MockMvc mockMvc;
	
	@Autowired
	ObjectMapper objectMapper;
	
	@MockBean
	LibraryEventProducer libraryEventProducer;
	
	@Test
	void postLibraryEventUnit() throws Exception {
		var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());
		
		when(libraryEventProducer.sendLibraryEventApproach3(isA(LibraryEvent.class))).thenReturn(null);
		
		mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryeventByApproach3")
				.content(json)
				.contentType(MediaType.APPLICATION_JSON))
		.andExpect(status().isCreated());
		
	}
	
	@Test
	void postLibraryEventUnitInvalid() throws Exception {
		var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook());
		
		when(libraryEventProducer.sendLibraryEventApproach3(isA(LibraryEvent.class))).thenReturn(null);
		
		var expectedErrorMessage = "book.bookId - must not be null, book.bookName - must not be blank";
		
		mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryeventByApproach3")
				.content(json)
				.contentType(MediaType.APPLICATION_JSON))
		.andExpect(status().is4xxClientError())
		.andExpect(content().string(expectedErrorMessage));
		
	}
}
