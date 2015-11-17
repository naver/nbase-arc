package com.navercorp.nbasearc.confmaster.server.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface Param {
    public enum ArgType {
        STRING_VARG, NULLABLE;
    }

    public ArgType type();
}
