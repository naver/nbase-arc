package com.navercorp.redis.cluster.spring.sample;

import com.navercorp.redis.cluster.spring.StringRedisClusterTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 *  @author Junhwan Oh
 */

@RestController
public class HelloController {

    private static String KEY = "SAMPLE";

    @Autowired
    private StringRedisClusterTemplate redisTemplate;

    @RequestMapping(value = "/get", method = RequestMethod.GET)
    @ResponseBody
    public Map<String, String> get() {
        String sample = redisTemplate.opsForValue().get(KEY);
        Map<String, String> re = new HashMap<String, String>();
        re.put("retrun", sample);
        return re;
    }

    @RequestMapping(value = "/put", method = RequestMethod.PUT)
    @ResponseBody
    public Map<String, String> put(@RequestBody Map<String, String> body) {
        String value = body.get("insert");
        redisTemplate.opsForValue().set(KEY, value);
        Map<String, String> re = new HashMap<String, String>();
        re.put("code", "200");
        return re;
    }
}
