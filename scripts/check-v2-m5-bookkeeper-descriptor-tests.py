#!/usr/bin/env python3
"""Reject descriptor scope/cap regressions and premature native lifecycle promotion."""
import copy
import importlib.util
from pathlib import Path
import unittest
spec = importlib.util.spec_from_file_location("descriptor", Path(__file__).with_name("check-v2-m5-bookkeeper-descriptor.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class DescriptorContractTest(unittest.TestCase):
    def test_current_sources(self):
        module.validate(module.ROOT)

    def test_rejects_removed_decoding_or_shared_budget_accounting(self):
        base = module.ROOT / "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction"
        names = ("KafkaSealedBookKeeperDescriptorV2.java", "KafkaSealedBookKeeperDescriptorCodecV2.java",
                 "KafkaBookKeeperArtifactAssemblerV2.java", "KafkaBookKeeperReadViewV2.java",
                 "KafkaSealedBookKeeperReaderV2.java", "KafkaBookKeeperCompactionPublicationV2.java")
        sources = {name: (base / name).read_text() for name in names}
        for predicate in ("KafkaRecordBatchCodecV1.parse(body)", "KafkaRecordBatchCodecV1.parseBounded",
                          "remainingRecords -=", "remainingBytes -="):
            with self.subTest(predicate=predicate):
                changed = sources.copy()
                self.assertIn(predicate, changed["KafkaBookKeeperReadViewV2.java"])
                changed["KafkaBookKeeperReadViewV2.java"] = changed["KafkaBookKeeperReadViewV2.java"].replace(
                    predicate, "removed_predicate")
                with self.assertRaises(ValueError):
                    module.validate_sources(changed)

    def test_rejects_changed_descriptor_domain_caps_and_suite_claims(self):
        for field in ("descriptorKind", "descriptorWire", "maximumDescriptorBytes", "physicalIndexLocators", "focusedSuites"):
            value = copy.deepcopy(module.EXPECTED)
            value.pop(field)
            with self.assertRaises(ValueError):
                module.validate_projection(value)

    def test_rejects_removed_predicates_or_premature_authority(self):
        for field, expected in module.EXPECTED.items():
            if isinstance(expected, bool):
                with self.subTest(field=field):
                    value = copy.deepcopy(module.EXPECTED)
                    value[field] = not expected
                    with self.assertRaises(ValueError):
                        module.validate_projection(value)

if __name__ == "__main__":
    unittest.main()
