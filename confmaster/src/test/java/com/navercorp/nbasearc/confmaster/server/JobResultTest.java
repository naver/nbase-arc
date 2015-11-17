package com.navercorp.nbasearc.confmaster.server;

import static org.junit.Assert.*;

import org.junit.Test;

import com.navercorp.nbasearc.confmaster.server.JobResult.CommonKey;

public class JobResultTest {

    @Test
    public void stringConvert() {
        assertEquals(CommonKey.STATE.toString(), "STATE");
        assertEquals(CommonKey.REQUEST.toString(), "REQUEST");
        assertEquals(CommonKey.START_TIME.toString(), "START_TIME");
        assertEquals(CommonKey.END_TIME.toString(), "END_TIME");
        assertEquals(CommonKey.USAGE.toString(), "USAGE");
    }

}
