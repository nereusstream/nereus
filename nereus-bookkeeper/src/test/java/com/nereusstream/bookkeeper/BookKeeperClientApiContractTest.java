/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Arrays;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.CreateAdvBuilder;
import org.apache.bookkeeper.client.api.DeleteBuilder;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.junit.jupiter.api.Test;

class BookKeeperClientApiContractTest {
    @Test
    void compilesAgainstPinnedPublic418Surface() throws Exception {
        assertThat(BookKeeper.class.getMethod("newCreateLedgerOp").getReturnType().getName())
                .isEqualTo("org.apache.bookkeeper.client.api.CreateBuilder");
        assertThat(CreateAdvBuilder.class.getMethod("withLedgerId", long.class)).isNotNull();
        assertThat(OpenBuilder.class.getMethod("withRecovery", boolean.class)).isNotNull();
        assertThat(DeleteBuilder.class.getMethods()).extracting(Method::getName).contains("withLedgerId");
        assertThat(WriteAdvHandle.class.getMethods()).extracting(Method::getName).contains("writeAsync");
        assertThat(ReadHandle.class.getMethods()).extracting(Method::getName).contains("readUnconfirmedAsync");
    }
}
