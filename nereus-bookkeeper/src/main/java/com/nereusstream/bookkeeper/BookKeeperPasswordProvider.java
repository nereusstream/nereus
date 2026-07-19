/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

/** Resolves a fresh caller-owned password copy; implementations must not log or cache returned bytes. */
@FunctionalInterface
public interface BookKeeperPasswordProvider {
    byte[] resolve(BookKeeperSecretRef reference);
}
