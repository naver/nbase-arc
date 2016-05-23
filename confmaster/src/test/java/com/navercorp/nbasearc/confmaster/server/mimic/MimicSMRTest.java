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

package com.navercorp.nbasearc.confmaster.server.mimic;

import static com.navercorp.nbasearc.confmaster.Constant.PGS_PING;
import static com.navercorp.nbasearc.confmaster.Constant.S2C_OK;
import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

public class MimicSMRTest {

    @Test
    public void operations() throws IOException {
        MimicSMR mimic = new MimicSMR();

        assertEquals("", mimic.execute("ping"));
        
        // Lconn
        assertEquals(S2C_OK, mimic.execute("role none"));
        assertEquals("+OK 0 " + mimic.getTimestamp(), mimic.execute(PGS_PING));
        
        mimic.init();
        assertEquals(S2C_OK, mimic.execute("role lconn"));
        assertEquals("+OK 1 " + mimic.getTimestamp(), mimic.execute(PGS_PING));
        
        // Master
        assertEquals("-ERR need <nid> <quorum policy> <rewind cseq>", mimic.execute("role master"));
        assertEquals(S2C_OK, mimic.execute("role master 0 0 0"));
        assertEquals("+OK 2 " + mimic.getTimestamp(), mimic.execute(PGS_PING));

        assertEquals("0", mimic.execute("getquorum"));
        assertEquals(S2C_OK, mimic.execute("setquorum 1"));
        assertEquals("1", mimic.execute("getquorum"));
        assertEquals(S2C_OK, mimic.execute("setquorum 2"));
        assertEquals("2", mimic.execute("getquorum"));
        assertEquals(S2C_OK, mimic.execute("setquorum 0"));
        assertEquals("0", mimic.execute("getquorum"));
        
        assertEquals(S2C_OK, mimic.execute("role lconn"));
        assertEquals("+OK 1 " + mimic.getTimestamp(), mimic.execute(PGS_PING));

        // Slave
        assertEquals("-ERR <nid> <host> <base port> [<rewind cseq>]", mimic.execute("role slave"));
        assertEquals(S2C_OK, mimic.execute("role slave 0 127.0.0.1 10000 0"));
        assertEquals("+OK 3 " + mimic.getTimestamp(), mimic.execute(PGS_PING));

        assertEquals("-ERR bad state:3", mimic.execute("getquorum"));
        assertEquals("-ERR bad state:3", mimic.execute("setquorum 0"));

        assertEquals(S2C_OK, mimic.execute("role lconn"));
        assertEquals("+OK 1 " + mimic.getTimestamp(), mimic.execute(PGS_PING));

        assertEquals("-ERR <nid> <host> <base port> [<rewind cseq>]", mimic.execute("role slave"));
        assertEquals(S2C_OK, mimic.execute("role slave 0 127.0.0.1 10000"));
        assertEquals("+OK 3 " + mimic.getTimestamp(), mimic.execute(PGS_PING));

        // Log sequence
        assertEquals("+OK log min:0 commit:0 max:0 be_sent:0", mimic.execute("getseq log"));
        mimic.setSeqLog(100, 1000, 10000, 5000);
        assertEquals("+OK log min:100 commit:1000 max:10000 be_sent:5000", mimic.execute("getseq log"));
    }

}
