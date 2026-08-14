# P6 S3 capability sources

Retrieved on 2026-08-14 from AWS documentation:

- [Amazon S3 multipart upload limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html) publish an
  Object maximum above the Nereus 4-GiB envelope, 10,000 parts, and the S3 part-size interval. Nereus deliberately
  admits only 4 GiB and 1,024 parts for P6.
- [Amazon S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) define
  `If-None-Match: *` create-if-absent behavior and conflict responses. The adapter resolves any uncertain/conflicting
  create only through an exact HEAD length/SHA proof.
- [PutObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) is the request surface exercised by
  `S3PulsarOffloadObjectStoreV1` for the admitted single-request body.

These published service limits qualify only the static capability interval. The executable receipt uses LocalStack and
therefore makes no Amazon S3 runtime-performance, durability, availability, or endorsement claim. The concrete
S3-compatible provider run pins [MinIO RELEASE.2025-09-07T16-13-09Z](https://github.com/minio/minio/releases/tag/RELEASE.2025-09-07T16-13-09Z)
and admits only that exact image digest.
