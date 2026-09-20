package com.kvstore;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class KVStore {

	public static void main(String[] args) {
		SpringApplication.run(KVStore.class, args);
		log.info("Distributed KV Store started");
	}

}
