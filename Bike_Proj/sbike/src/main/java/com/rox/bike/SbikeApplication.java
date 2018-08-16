package com.rox.bike;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 默认只会扫描同级目录
 */
@SpringBootApplication
public class SbikeApplication {

	public static void main(String[] args) {
		SpringApplication.run(SbikeApplication.class, args);
	}
}
