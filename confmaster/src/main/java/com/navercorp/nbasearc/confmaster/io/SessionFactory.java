package com.navercorp.nbasearc.confmaster.io;

public interface SessionFactory {

    Session session();

    SessionHandler handler(EventSelector eventSelctor);

}
