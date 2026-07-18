/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.ObjectKeyPrefix;

/** Closed runtime registration of one product-owned object prefix and its strict key inverse. */
public interface ObjectInventoryFamily {
    String familyId();

    ObjectKeyPrefix prefix();

    ObjectInventoryKey parse(ObjectKey key);
}
