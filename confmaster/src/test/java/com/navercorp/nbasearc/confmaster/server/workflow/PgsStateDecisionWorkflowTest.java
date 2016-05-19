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
