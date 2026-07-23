/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.core.read.ParquetV2CompactedTargetReader;
import java.util.List;
import org.junit.jupiter.api.Test;

class PhysicalFormatCapabilityRegistryTest {
    @Test
    void digestIsCanonicalAndOldCapabilityCannotAdmitV2Writes() {
        PhysicalFormatCapability ncp2 = new PhysicalFormatCapability(
                ParquetV2CompactedTargetReader.NCP2_KEY,
                ReadView.COMMITTED,
                PayloadFormat.KAFKA_RECORD_BATCH,
                true,
                true);
        PhysicalFormatCapability ntc2 = new PhysicalFormatCapability(
                ParquetV2CompactedTargetReader.NTC2_KEY,
                ReadView.TOPIC_COMPACTED,
                PayloadFormat.KAFKA_RECORD_BATCH,
                true,
                true);
        PhysicalFormatCapabilityRegistry required =
                new PhysicalFormatCapabilityRegistry(List.of(ncp2, ntc2));
        PhysicalFormatCapabilityRegistry reversed =
                new PhysicalFormatCapabilityRegistry(List.of(ntc2, ncp2));
        PhysicalFormatCapabilityRegistry oldBroker =
                new PhysicalFormatCapabilityRegistry(List.of());

        assertThat(required.digestSha256()).isEqualTo(reversed.digestSha256());
        assertThat(required.digestSha256().value())
                .isEqualTo("3c99feb81221497e1e1e7401766ecad898ace0cce2a68312c91bbec25b09bace");
        assertThatThrownBy(() -> oldBroker.requireSupersetOf(required))
                .isInstanceOfSatisfying(NereusException.class,
                        error -> assertThat(error.code()).isEqualTo(ErrorCode.UNSUPPORTED_READ_TARGET));
        reversed.requireSupersetOf(required);
    }
}
