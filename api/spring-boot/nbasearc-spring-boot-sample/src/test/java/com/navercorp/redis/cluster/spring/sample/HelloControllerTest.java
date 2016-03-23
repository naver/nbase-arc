package com.navercorp.redis.cluster.spring.sample;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.client.RestTemplate;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Junhwan Oh
 */


@RunWith(SpringJUnit4ClassRunner.class)   // 1
@SpringApplicationConfiguration(classes = SampleSimpleApplication.class)   // 2
@WebAppConfiguration   // 3
@IntegrationTest("server.port:0")   // 4
public class HelloControllerTest {
    ObjectMapper mapper = new ObjectMapper();
    @Value("${local.server.port}")
    private int port;

    private RestTemplate template;

    private String hostName;
    @Before
    public void setUp() throws Exception {
        template = new TestRestTemplate();
        hostName = "http://localhost:" + port;
    }
    @Test
    public void getHello() throws Exception {
        String testValue = "SAMPLE";
        Map<String, Object> in = new HashMap<String, Object>();
        in.put("insert",testValue);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> requestUpdate = new HttpEntity<String>(mapper.writeValueAsString(in), headers);

        ResponseEntity<Void> response1 = template.exchange(hostName + "/put", HttpMethod.PUT, requestUpdate, Void.class);
        assertThat(response1.getStatusCode(), equalTo(HttpStatus.OK));

        ResponseEntity<String> response2 = template.getForEntity(hostName + "/get", String.class);
        String body = response2.getBody();
        Map<String, Object> re = mapper.readValue(body, new TypeReference<Map<String, Object>>() {
        });
        assertThat((String) re.get("retrun"), equalTo(testValue));

    }
}