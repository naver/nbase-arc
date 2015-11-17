package com.navercorp.nbasearc.confmaster.server.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CommandMapping {
    public String name();

    public String usage() default "";

    public ArityType arityType() default ArityType.EQUAL;
}
