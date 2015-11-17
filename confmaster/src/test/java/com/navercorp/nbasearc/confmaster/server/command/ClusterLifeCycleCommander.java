package com.navercorp.nbasearc.confmaster.server.command;

import static com.navercorp.nbasearc.confmaster.Constant.S2C_OK;
import static org.junit.Assert.*;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.server.JobResult;

public class ClusterLifeCycleCommander extends Thread {
    
    public static final String[][] templateOperations = {
            {"pm_add cluster_life_cycle_host 127.0.0.1", S2C_OK},
            {"cluster_add cluster_life_cycle 0:1", S2C_OK},
            {"pg_add cluster_life_cycle 0", S2C_OK},
            {"pg_add cluster_life_cycle 1", S2C_OK},
            {"pg_add cluster_life_cycle 2", S2C_OK},
            {"pg_add cluster_life_cycle 3", S2C_OK},
            {"pg_ls cluster_life_cycle", "{\"list\":[\"0\", \"1\", \"2\", \"3\"]}"},
            {"slot_set_pg cluster_life_cycle 0:2047 0", S2C_OK},
            {"slot_set_pg cluster_life_cycle 2048:4095 1", S2C_OK},
            {"slot_set_pg cluster_life_cycle 4095:6143 2", S2C_OK},
            {"slot_set_pg cluster_life_cycle 6143:8191 3", S2C_OK},
            {"pgs_add cluster_life_cycle 0 0 cluster_life_cycle_host 127.0.0.1 7000 7009", S2C_OK},
            {"pgs_add cluster_life_cycle 1 0 cluster_life_cycle_host 127.0.0.1 7010 7019", S2C_OK},
            {"pgs_add cluster_life_cycle 10 1 cluster_life_cycle_host 127.0.0.1 7020 7029", S2C_OK},
            {"pgs_add cluster_life_cycle 11 1 cluster_life_cycle_host 127.0.0.1 7030 7039", S2C_OK},
            {"pgs_add cluster_life_cycle 20 2 cluster_life_cycle_host 127.0.0.1 7040 7049", S2C_OK},
            {"pgs_add cluster_life_cycle 21 2 cluster_life_cycle_host 127.0.0.1 7050 7059", S2C_OK},
            {"pgs_add cluster_life_cycle 30 3 cluster_life_cycle_host 127.0.0.1 7060 7069", S2C_OK},
            {"pgs_add cluster_life_cycle 31 3 cluster_life_cycle_host 127.0.0.1 7070 7079", S2C_OK},
            {"pgs_ls cluster_life_cycle", "{\"list\":[\"0\", \"1\", \"10\", \"11\", \"20\", \"21\", \"30\", \"31\"]}"},
            {"gw_add cluster_life_cycle 1 cluster_life_cycle_host 127.0.0.1 6000", S2C_OK},
            {"gw_add cluster_life_cycle 2 cluster_life_cycle_host 127.0.0.1 6010", S2C_OK},
            {"gw_ls cluster_life_cycle", "{\"list\":[\"1\", \"2\"]}"},
            {"gw_del cluster_life_cycle 1", S2C_OK},
            {"gw_del cluster_life_cycle 2", S2C_OK},
            {"pgs_del cluster_life_cycle 0", S2C_OK},
            {"pgs_del cluster_life_cycle 1", S2C_OK},
            {"pgs_del cluster_life_cycle 10", S2C_OK},
            {"pgs_del cluster_life_cycle 11", S2C_OK},
            {"pgs_del cluster_life_cycle 20", S2C_OK},
            {"pgs_del cluster_life_cycle 21", S2C_OK},
            {"pgs_del cluster_life_cycle 30", S2C_OK},
            {"pgs_del cluster_life_cycle 31", S2C_OK},
            {"pg_del cluster_life_cycle 0", S2C_OK},
            {"pg_del cluster_life_cycle 1", S2C_OK},
            {"pg_del cluster_life_cycle 2", S2C_OK},
            {"pg_del cluster_life_cycle 3", S2C_OK},
            {"cluster_del cluster_life_cycle", S2C_OK},
            {"pm_del cluster_life_cycle_host", S2C_OK}
    };
    private BasicSetting basicSetting;
    
    public ClusterLifeCycleCommander(BasicSetting basicSetting) {
        this.basicSetting = basicSetting;
    }
    
    public static String[][] makeOperations(long unique) {
        String[][] operations = new String[templateOperations.length][2];
        for (int i = 0; i < templateOperations.length; i++) {
            operations[i][0] = templateOperations[i][0].replace(
                    "cluster_life_cycle", "cluster_life_cycle" + unique);
            operations[i][1] = templateOperations[i][1];
        }
        return operations;
    }
    
    @Override
    public void run() {
        String[][] operations = makeOperations(Thread.currentThread().getId());
        for (int i = 0; i < 3; i ++) {
            for (String[] operation : operations) {
                final String command = operation[0];
                final String reply = operation[1];
                JobResult result = null;
                try {
                    result = basicSetting.doCommand(command);
                } catch (Exception e) {
                }
                assertTrue(
                        "check result of \"" + command + "\"",
                        result.getExceptions().isEmpty());
                assertEquals(
                        "check result of \"" + command + "\"",
                        reply,
                        result.getMessages().get(0));
            }
        }
    }
    
}