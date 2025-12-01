package com.bfl.qualifier.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import model.WebhookResponse;

@Service
public class WebhookService implements ApplicationRunner {

	private static final Logger log = LoggerFactory.getLogger(WebhookService.class);

	private final RestTemplate restTemplate;

	public WebhookService(RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
	}

	@Override
	public void run(ApplicationArguments args) {
		log.info("Starting flow...");
		try {
			executeFlow();
		} catch (Exception e) {
			log.error("Error", e);
		}
	}

	public void executeFlow() {
		String url = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

		Map<String, String> request = new HashMap<>();
		request.put("name", "Appani Pranathi");
		request.put("regNo", "22BCE7658");
		request.put("email", "pranathi.22bce7658@vitapstudent.ac.in");

		WebhookResponse response = restTemplate.postForObject(url, request, WebhookResponse.class);

		if (response == null) {
			throw new RuntimeException("Failed to get webhook response");
		}

		String webhookUrl = response.getWebhook();
		String accessToken = response.getAccessToken();

		log.info("Received webhook URL: {}", webhookUrl);
		log.info("Access token: {}", accessToken);

		String finalQuery = "WITH AgeData AS ( " +
                    "    SELECT " +
                    "        d.DEPARTMENT_ID, " +
"        d.DEPARTMENT_NAME, " +
"        e.EMP_ID, " +
"        e.FIRST_NAME, " +
"        e.LAST_NAME, " +
"        FLOOR(DATEDIFF(CURDATE(), e.DOB) / 365) AS AGE, " +
"        p.AMOUNT " +
"    FROM EMPLOYEE e " +
"    JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID " +
"    JOIN PAYMENTS p ON e.EMP_ID = p.EMP_ID " +
"    WHERE p.AMOUNT > 70000 " +
") " +
"SELECT " +
"    DEPARTMENT_NAME, " +
"    AVG(AGE) AS AVERAGE_AGE, " +
"    GROUP_CONCAT(CONCAT(FIRST_NAME, ' ', LAST_NAME) " +
"                 ORDER BY FIRST_NAME, LAST_NAME " +
"                 SEPARATOR ', ') AS EMPLOYEE_LIST " +
"FROM ( " +
"    SELECT " +
"        DEPARTMENT_ID, " +
"        DEPARTMENT_NAME, " +
"        FIRST_NAME, " +
"        LAST_NAME, " +
"        AGE, " +
"        ROW_NUMBER() OVER ( " +
"            PARTITION BY DEPARTMENT_ID " +
"            ORDER BY FIRST_NAME, LAST_NAME " +
"        ) AS rn " +
"    FROM AgeData " +
") ranked " +
"WHERE rn <= 10 " +
"GROUP BY DEPARTMENT_ID, DEPARTMENT_NAME " +
"ORDER BY DEPARTMENT_ID DESC";


		log.debug("Final SQL Query: {}", finalQuery);

		Map<String, String> body = Collections.singletonMap("finalQuery", finalQuery);

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.add("Authorization", accessToken);

		HttpEntity<Map<String, String>> entity = new HttpEntity<>(body, headers);

		ResponseEntity<String> result = restTemplate.exchange(webhookUrl, HttpMethod.POST, entity, String.class);

		log.info("Webhook response: {}", result.getBody());
	}
}
