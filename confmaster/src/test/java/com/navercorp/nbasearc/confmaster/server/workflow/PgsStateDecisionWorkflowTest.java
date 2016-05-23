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

package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.repository.znode.OpinionData;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;
import com.navercorp.nbasearc.confmaster.server.cluster.UsedOpinionSet;
import com.navercorp.nbasearc.confmaster.server.workflow.PGSStateDecisionWorkflow.MakeDecisionResult;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class PgsStateDecisionWorkflowTest {

    @Autowired
    ApplicationContext context;
    
    @Test
    public void test() throws SecurityException, NoSuchMethodException,
            IllegalArgumentException, IllegalAccessException,
            InvocationTargetException {
        HeartbeatTarget target = mock(HeartbeatTarget.class); 
        when(target.getUsedOpinions()).thenReturn(new UsedOpinionSet());
        
        PGSStateDecisionWorkflow wf = new PGSStateDecisionWorkflow(target, context);
        Class[] args = new Class[2];
        args[0] = List.class;
        args[1] = long.class;
        Method md = PGSStateDecisionWorkflow.class.getDeclaredMethod("makeDecision", args);
        md.setAccessible(true);
        
        List<OpinionData> opinions = new ArrayList<OpinionData>();
        OpinionData o = new OpinionData();
        o.setName("0");
        o.setOpinion(PGS_ROLE_MASTER);
        o.setStatetimestamp(100);
        o.setVersion(10);
        opinions.add(o);
        
        MakeDecisionResult result = (MakeDecisionResult) md.invoke(wf, opinions, 100);
        assertEquals(PGS_ROLE_MASTER, result.newView);
    }

}
