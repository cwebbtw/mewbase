/*
 *
 *  Copyright 2014 Red Hat, Inc.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *      The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *
 *      The Apache License v2.0 is available at
 *      http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 *
 *
 */

package io.mewbase.util;

import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static junit.framework.Assert.assertNotSame;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.fail;

/**
 * @author Nige
 */
@RunWith(VertxUnitRunner.class)
public class ExecutorTest {

    private final ExecutorService stexec = Executors.newSingleThreadExecutor();

    @Test
    public void CompFutCapturesException() throws Exception {

        CompletableFuture fut = CompletableFuture.supplyAsync( () -> {
            throw new RuntimeException("Help Help");
        },stexec);
        fut.handle( (val,exp) -> {
            if (exp == null) fail("Exception not handled");
            return val;
        });
    }

}
