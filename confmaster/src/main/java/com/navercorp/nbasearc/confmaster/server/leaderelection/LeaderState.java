package com.navercorp.nbasearc.confmaster.server.leaderelection;

public class LeaderState {

    private static ElectionState state = ElectionState.INIT;

    public static void init() {
        state = ElectionState.INIT;
    }

    public static boolean isLeader() {
        return state.equals(ElectionState.LEADER);
    }

    public static boolean isFollower() {
        return state.equals(ElectionState.FOLLOWER);
    }
    
    public static ElectionState getPrevilege() {
        return state;
    }

    public static void setLeader() {
        state = ElectionState.LEADER;
    }

    public static void setFollower() {
        state = ElectionState.FOLLOWER;
    }
    
    public enum ElectionState {
        INIT(1),
        FOLLOWER(2),
        LEADER(3);
        
        private final int state;
        
        private ElectionState(int state) {
            this.state = state;
        }

        public boolean isGreaterOrEqual(ElectionState e) {
            return this.state >= e.state;
        }
        
        @Override
        public String toString() {
            switch (this) {
            case INIT:
                return "INIT";
            case LEADER:
                return "LEADER";
            case FOLLOWER:
                return "FOLLOWER";
            default:
                return "UNKONW";
            }
        }
    }

}
