/*
 * Copyright 2015 Naver Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.client;

import static org.junit.Assert.*;

import java.io.IOException;

import static com.navercorp.client.Client.Code.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.navercorp.client.Client.Result;

public class ClientTest {

    static Client c;
    
    @BeforeClass 
    public static void beforeClass() throws Exception {
        c = new Client("127.0.0.1:2181", 10000);
    }
    
    @AfterClass
    public static void afterClass() throws InterruptedException {
        c.close();
    }
    
    @Test(expected=IOException.class)
    public void connectionTimeout() throws Exception {
        new Client("192.0.2.2:12345", 1000);    // test-net-1 in rfc5737
    }
    
    @Test
    public void invalidCommand() throws Exception {
        Result r = c.execute(new String[]{"invalid"});
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, INVALID_COMMAND);
    }
    
    @Test
    public void create() throws Exception {
        Result r = c.execute("create /testcreate".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, INVALID_ARGUMENT);
        
        r = c.execute("create /testcreate value".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
        
        r = c.execute("create /testcreate value".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NODE_EXISTS);
        
        r = c.execute("create /testcreate_noparent/test value".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NO_NODE);
        
        r = c.execute("del /testcreate".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
    }
    
    @Test
    public void ls() throws Exception {
        Result r = c.execute("ls /testcreate invalidargument".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, INVALID_ARGUMENT);
        
        r = c.execute("ls /".split(" "));
        assertNotEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);

        r = c.execute("ls /testls_nonode".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NO_NODE);

        r = c.execute("ls /testls_noparent/test".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NO_NODE);
    }
    
    @Test
    public void rmr() throws Exception {
        Result r = c.execute("rmr /testrmr invalidargument".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, INVALID_ARGUMENT);
        
        r = c.execute("create /testrmr value".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
        
        r = c.execute("create /testrmr/1 value".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
        
        r = c.execute("create /testrmr/1/2 value".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
        
        r = c.execute("create /testrmr/1/2/3 value".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
        
        r = c.execute("rmr /testrmr".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
        
        r = c.execute("rmr /testrmr_nonode".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NO_NODE);

        r = c.execute("rmr /testrmr_noparent/test".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NO_NODE);
    }
    
    @Test
    public void del() throws Exception {
        Result r = c.execute("del /testdel invalidargument".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, INVALID_ARGUMENT);
        
        r = c.execute("create /testdel value".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);

        r = c.execute("create /testdel/test value".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
        
        r = c.execute("del /testdel".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NOT_EMPTY_NODE);

        r = c.execute("del /testdel/test".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);

        r = c.execute("del /testdel".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
        
        r = c.execute("del /testdel_nonode".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NO_NODE);

        r = c.execute("del /testdel_noparent/test".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NO_NODE);
    }
    
    @Test
    public void set() throws Exception {
        Result r = c.execute("set /testset".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, INVALID_ARGUMENT);
        
        r = c.execute("create /testset value".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
        
        r = c.execute("set /testset value2".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
        
        r = c.execute("set /testset_nonde value".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NO_NODE);

        r = c.execute("set /testset_noparent/no_node value".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NO_NODE);

        r = c.execute("del /testset".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
    }
    
    @Test
    public void get() throws Exception {
        Result r = c.execute("get /testget invalidargument".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, INVALID_ARGUMENT);
        
        r = c.execute("create /testget value".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
        
        r = c.execute("get /testget".split(" "));
        assertEquals(r.message, "value");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);

        r = c.execute("get /testget_nonode".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NO_NODE);

        r = c.execute("get /testget_noparent/test".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, ZK_NO_NODE);

        r = c.execute("del /testget".split(" "));
        assertEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
    }
    
    @Test
    public void help() {
        Result r = c.execute("help".split(" "));
        assertNotEquals(r.message, "");
        assertEquals(r.error, "");
        assertEquals(r.exitcode, OK);
    }
    
    @Test
    public void error() {
        Result r = c.execute("get invalid_path".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, INVALID_ARGUMENT);
        
        r = c.execute("set /test value wrong_number_of_arguments".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, INVALID_ARGUMENT);

        r = c.execute("".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, INVALID_COMMAND);
        
        r = c.execute("wrong_command".split(" "));
        assertEquals(r.message, "");
        assertNotEquals(r.error, "");
        assertEquals(r.exitcode, INVALID_COMMAND);
    }
    
    @Test
    public void usage() {
        assertTrue(Client.getUsage().length() > 0);
    }
    
}
