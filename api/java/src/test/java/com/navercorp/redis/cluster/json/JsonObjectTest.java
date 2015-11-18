/*
 * Copyright 2015 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.navercorp.redis.cluster.json;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author jaehong.kim
 */
public class JsonObjectTest {

    @Test
    public void json() {
        String data = "{\"ip\":\"1.1.1.1\", \"port\":6000}";
        JsonObject jsonObject = JsonObject.readFrom(data);
        assertEquals("1.1.1.1", jsonObject.get("ip").asString());
        assertEquals(6000, jsonObject.get("port").asInt());
    }

    @Test
    public void readJson() throws Exception {
        String data = "[{\"gw_id\":1, \"affinity\":\"A10R5N1\"}]";

        JsonArray jsonArray = JsonArray.readFrom(data);
        for (JsonValue value : jsonArray) {
            JsonObject jsonObject = value.asObject();
            assertEquals(1, jsonObject.get("gw_id").asInt());
            assertEquals("A10R5N1", jsonObject.get("affinity").asString());

        }

    }

}
