package com.navercorp.nbasearc.confmaster.logger;

public class EpochMsgDecorator implements MsgDecorator {

    final long epoch;

    public EpochMsgDecorator(long epoch) {
        this.epoch = epoch;
    }

    @Override
    public String decorateMessage(String msg) {
        return String.format("%d %s", epoch, msg);
    }

}
