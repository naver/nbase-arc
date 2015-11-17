package com.navercorp.nbasearc.confmaster.server.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState.ElectionState;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface WorkflowMapping {
    public String name();

    public ArityType arityType() default ArityType.EQUAL;

    public ElectionState privilege();
}
