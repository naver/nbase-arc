package com.navercorp.nbasearc.confmaster.faultinjection;

import static com.navercorp.nbasearc.confmaster.Constant.*;

import org.springframework.beans.factory.annotation.Autowired;

import com.navercorp.nbasearc.confmaster.server.command.FaultInjectionService;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;

public class FaultInjectionServiceImpl extends FaultInjectionService {
    
    @Autowired
    RARoleLconnFI raRoleLconn;
    
    @Autowired
    RASetquorumFI raSetquorum;
    
    @Autowired
    MERoleMasterFI meRoleMaster;
    
    @Autowired
    MERoleLconnFI meRoleLconn;
    
    @Autowired
    QASetquorumFI qaSetquorum;
    
    @Autowired
    BJRoleSlaveFI bjRoleSlave;

    @Autowired
    YJRoleSlaveFI yjRoleSlave;
    
    @Autowired
    MGSetquorumFI mgSetquorum;
    
    @CommandMapping(name = "fi_add", usage = "fi_add <workflow> <where> <successFail> <count>\r\n"
            + "workflow: RA, QA, ME, YJ, BJ, MG\r\n"
            + "where: master, slave, lconn, setquorum")
    public String fiAdd(String workflow, String where, boolean successFail, int count) {
        workflow = workflow.toLowerCase();
        where = where.toLowerCase();
        
        if (workflow.equals("ra")) {
            if (where.equals("lconn")) {
                raRoleLconn.setCount(count);
                raRoleLconn.setSuccessFail(successFail);
                return S2C_OK;
            } else if (where.equals("setquorum")) {
                raSetquorum.setCount(count);
                raSetquorum.setSuccessFail(successFail);
                return S2C_OK;
            }
        } else if (workflow.equals("me")) {
            if (where.equals("lconn")) {
                meRoleLconn.setCount(count);
                meRoleLconn.setSuccessFail(successFail);
                return S2C_OK;
            } else if (where.equals("master")) {
                meRoleMaster.setCount(count);
                meRoleMaster.setSuccessFail(successFail);
                return S2C_OK;
            }
        } else if (workflow.equals("qa")) {
            if (where.equals("setquorum")) {
                qaSetquorum.setCount(count);
                qaSetquorum.setSuccessFail(successFail);
                return S2C_OK;
            }
        } else if (workflow.equals("bj")) {
            if (where.equals("slave")) {
                bjRoleSlave.setCount(count);
                bjRoleSlave.setSuccessFail(successFail);
                return S2C_OK;
            }
        } else if (workflow.equals("yj")) {
            if (where.equals("slave")) {
                yjRoleSlave.setCount(count);
                yjRoleSlave.setSuccessFail(successFail);
                return S2C_OK;
            }
        } else if (workflow.equals("mg")) {
            if (where.equals("setquorum")) {
                mgSetquorum.setCount(count);
                mgSetquorum.setSuccessFail(successFail);
                return S2C_OK;
            }
        }

        return ERROR;
    }

    @LockMapping(name = "fi_add")
    public void fiAddLock() {
    }

    @CommandMapping(name = "fi_count", usage = "fi_count <workflow> <where>")
    public String fiCount(String workflow, String where) {
        workflow = workflow.toLowerCase();
        where = where.toLowerCase();

        if (workflow.equals("ra")) {
            if (where.equals("lconn")) {
                return String.valueOf(raRoleLconn.getCount());
            } else if (where.equals("setquorum")) {
                return String.valueOf(raSetquorum.getCount());
            }
        } else if (workflow.equals("me")) {
            if (where.equals("lconn")) {
                return String.valueOf(meRoleLconn.getCount());
            } else if (where.equals("master")) {
                return String.valueOf(meRoleMaster.getCount());
            }
        } else if (workflow.equals("qa")) {
            if (where.equals("setquorum")) {
                return String.valueOf(qaSetquorum.getCount());
            }
        } else if (workflow.equals("bj")) {
            if (where.equals("slave")) {
                return String.valueOf(bjRoleSlave.getCount());
            }
        } else if (workflow.equals("yj")) {
            if (where.equals("slave")) {
                return String.valueOf(yjRoleSlave.getCount());
            }
        } else if (workflow.equals("mg")) {
            if (where.equals("setquorum")) {
                return String.valueOf(mgSetquorum.getCount());
            }
        }

        return ERROR;
    }

    @LockMapping(name = "fi_count")
    public void fiCountLock() {
    }
}
