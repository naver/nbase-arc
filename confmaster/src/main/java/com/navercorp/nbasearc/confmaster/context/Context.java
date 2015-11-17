package com.navercorp.nbasearc.confmaster.context;

import java.util.concurrent.Callable;

public interface Context<T> extends Callable<T> {
}
