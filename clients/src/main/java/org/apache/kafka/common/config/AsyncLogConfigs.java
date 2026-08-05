/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.config;

public class AsyncLogConfigs {
    public static final String LOG_ASYNC_MODE = "log.async.mode.enable";

    // ----------------------- managed ledger config --------------------------------
    public static final String BOOKKEEPER_ENSEMBLE_SIZE = "bookkeeper.ensemble.size";
    public static final String BOOKKEEPER_WRITE_QUORUM_SIZE = "bookkeeper.write.quorum.size";
    public static final String BOOKKEEPER_ACK_QUORUM_SIZE = "bookkeeper.ack.quorum.size";
    public static final String BOOKKEEPER_LEDGER_DELETE_MAX_CONCURRENT_REQUESTS = "bookkeeper.ledger.delete.max.concurrent.requests";
    public static final String BOOKKEEPER_LEDGER_DELETION_THREADS = "bookkeeper.ledger.deletion.threads";
    public static final String BOOKKEEPER_DIGEST_TYPE = "bookkeeper.digest.type";
    public static final String BOOKKEEPER_PASSWORD = "bookkeeper.password";
    public static final String BOOKKEEPER_MAX_ENTRIES_PER_LEDGER = "bookkeeper.max.entries.per.ledger";
    public static final String BOOKKEEPER_MAX_BYTES_PER_LEDGER_MB = "bookkeeper.max.bytes.per.ledger.mb";
    public static final String BOOKKEEPER_MIN_ROLLOVER_TIME_MINUTES = "bookkeeper.min.rollover.time.minutes";
    public static final String BOOKKEEPER_MAX_ROLLOVER_TIME_MINUTES = "bookkeeper.max.rollover.time.minutes";
    public static final String BOOKKEEPER_METADATA_OPERATION_TIMEOUT_SECONDS = "bookkeeper.metadata.operation.timeout.seconds";
    public static final String BOOKKEEPER_READ_ENTRY_TIMEOUT_SECONDS = "bookkeeper.read.entry.timeout.seconds";
    public static final String BOOKKEEPER_ADD_ENTRY_TIMEOUT_SECONDS = "bookkeeper.add.entry.timeout.seconds";
    public static final String BOOKKEEPER_METADATA_ENSEMBLE_SIZE = "bookkeeper.metadata.ensemble.size";
    public static final String BOOKKEEPER_METADATA_WRITE_QUORUM_SIZE = "bookkeeper.metadata.write.quorum.size";
    public static final String BOOKKEEPER_METADATA_ACK_QUORUM_SIZE = "bookkeeper.metadata.ack.quorum.size";
    public static final String BOOKKEEPER_METADATA_MAX_ENTRIES_PER_LEDGER = "bookkeeper.metadata.max.entries.per.ledger";
    public static final String BOOKKEEPER_LEDGER_ROLLOVER_TIMEOUT_SECONDS = "bookkeeper.ledger.rollover.timeout.seconds";
    public static final String BOOKKEEPER_RETENTION_TIME_SECONDS = "bookkeeper.retention.time.seconds";
    public static final String BOOKKEEPER_RETENTION_SIZE_MB = "bookkeeper.retention.size.mb";
    public static final String BOOKKEEPER_AUTO_SKIP_NONRECORVERABLE_DATA = "bookkeeper.auto.skip.nonrecoverable.data";
    public static final String BOOKKEEPER_LEDGER_FORCE_RECOVERY = "bookkeeper.ledger.force.recovery";
    public static final String BOOKKEEPER_INACTIVE_LEDGER_ROLLOVER_TIME_SECONDS = "bookkeeper.inactive.ledger.rollover.time.seconds";
    public static final String BOOKKEEPER_METADATA_STORE_URL = "bookkeeper.metadata.store.url";

    // ------------------------ managed ledger factory config --------------------------------
    public static final String ML_MAX_CACHE_SIZE_MB = "managed.ledger.cache.size.mb";
    public static final String ML_CACHE_EVICTION_WATERMARK = "managed.ledger.cache.eviction.watermark";
    public static final String ML_NUM_SCHEDULER_THREADS = "managed.ledger.num.scheduler.threads";
    public static final String ML_CACHE_EVICTION_INTERVAL_MS = "managed.ledger.cache.eviction.interval.ms";
    public static final String ML_CACHE_EVICTION_TIME_THRESHOLD_MS = "managed.ledger.cache.eviction.time.threshold.ms";
    public static final String ML_COPY_ENTRIES_IN_CACHE = "managed.ledger.copy.entries.in.cache";
    public static final String ML_MAX_READ_IN_FLIGHT_SIZE_MB = "managed.ledger.max.read.in.flight.size.mb";
    public static final String ML_MAX_READS_INFLIGHT_SIZE = "managed.ledger.max.reads.inflight.size";
    public static final String ML_MAX_READ_INFLIGHT_PERMITS_ACQUIRE_TIMEOUT_MS = "managed.ledger.max.read.inflight.permits.acquire.timeout.ms";
    public static final String ML_MAX_READ_INFLIGHT_PERMITS_ACQUIRE_QUEUE_SIZE = "managed.ledger.max.read.inflight.permits.acquire.queue.size";
    public static final String ML_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS = "managed.ledger.prometheus.stats.latency.rollover.seconds";
    public static final String ML_TRACE_TASK_EXECUTION = "managed.ledger.trace.task.execution";
    public static final String ML_INFO_COMPRESSION_TYPE = "managed.ledger.info.compression.type";
    public static final String ML_INFO_COMPRESSION_THRESHOLD_BYTES = "managed.ledger.info.compression.threshold.bytes";
    public static final String ML_STATS_PERIOD_SECONDS = "managed.ledger.info.stats.period.seconds";

    //---------------------- bookkeeper client config --------------------------------
    public static final String BOOKKEEPER_CLIENT_AUTHENTICATION_PLUGIN = "bookkeeper.client.authentication.plugin";
    public static final String BOOKKEEPER_CLIENT_AUTHENTICATION_PARAMETERS = "bookkeeper.client.authentication.parameters";
    public static final String BOOKKEEPER_CLIENT_TLS_AUTHENTICATION = "bookkeeper.client.tls.authentication";
    public static final String BOOKKEEPER_CLIENT_TLS_CERTIFICATE_FILE_PATH = "bookkeeper.client.tls.certificate.file.path";
    public static final String BOOKKEEPER_CLIENT_TLS_KEY_FILE_PATH = "bookkeeper.client.tls.key.file.path";
    public static final String BOOKKEEPER_CLIENT_TLS_KEY_FILE_TYPE = "bookkeeper.client.tls.key.file.type";
    public static final String BOOKKEEPER_CLIENT_TLS_KEY_STORE_PASSWORD_PATH = "bookkeeper.client.tls.key.store.password.path";
    public static final String BOOKKEEPER_CLIENT_TLS_PROVIDER_FACTORY_CLASS = "bookkeeper.client.tls.provider.factory.class";
    public static final String BOOKKEEPER_CLIENT_TLS_TRUST_CERTS_FILE_PATH = "bookkeeper.client.tls.trust.certs.file.path";
    public static final String BOOKKEEPER_CLIENT_TLS_TRUST_CERT_TYPES = "bookkeeper.client.tls.trust.cert.types";
    public static final String BOOKKEEPER_CLIENT_TLS_TRUST_STORE_PASSWORD_PATH = "bookkeeper.client.tls.trust.store.password.path";
    public static final String BOOKKEEPER_CLIENT_TLS_CERT_FILES_REFRESH_DURATION_SECONDS = "bookkeeper.client.tls.cert.files.refresh.duration.seconds";
    public static final String BOOKKEEPER_CLIENT_ENABLE_BUSY_WAIT = "bookkeeper.client.enable.busy.wait";
    public static final String BOOKKEEPER_CLIENT_NUM_WORKER_THREADS = "bookkeeper.client.num.worker.threads";
    public static final String BOOKKEEPER_CLIENT_THROTTLE_VALUE = "bookkeeper.client.throttle.value";
    public static final String BOOKKEEPER_CLIENT_METADATA_SESSION_TIMEOUT_MILLIS = "bookkeeper.client.metadata.session.timeout.millis";
    public static final String BOOKKEEPER_CLIENT_TIMEOUT_SECONDS = "bookkeeper.client.timeout.seconds";
    public static final String BOOKKEEPER_CLIENT_SPECULATIVE_READ_TIMEOUT_MILLIS = "bookkeeper.client.speculative.read.timeout.millis";
    public static final String BOOKKEEPER_CLIENT_NUMBER_OF_CHANNEL_PER_BOOKIE = "bookkeeper.client.number.of.channels.per.bookie";
    public static final String BOOKKEEPER_CLIENT_USE_V2_WIRE_PROTOCOL = "bookkeeper.client.use.v2.wire.protocol";
    public static final String BOOKKEEPER_CLIENT_ENABLE_STICKY_READS = "bookkeeper.client.enable.sticky.reads";
    public static final String BOOKKEEPER_CLIENT_NETTY_MAX_FRAME_SIZE_BYTES = "bookkeeper.client.netty.max.frame.size.bytes";
    public static final String BOOKKEEPER_CLIENT_DISK_WEIGHT_BASED_PLACEMENT_ENABLED = "bookkeeper.client.disk.weight.based.placement.enabled";
    public static final String BOOKKEEPER_CLIENT_METADATA_SERVICE_URL = "bookkeeper.client.metadata.service.url";
    public static final String BOOKKEEPER_CLIENT_HEALTH_CHECK_ENABLED = "bookkeeper.client.health.check.enabled";
    public static final String BOOKKEEPER_CLIENT_HEALTH_CHECK_INTERVAL_SECONDS = "bookkeeper.client.health.check.interval.seconds";
    public static final String BOOKKEEPER_CLIENT_HEALTH_CHECK_ERROR_THRESHOLD_PER_INTERVAL = "bookkeeper.client.health.check.error.threshold.per.interval";
    public static final String BOOKKEEPER_CLIENT_HEALTH_CHECK_QUARANTINE_TIME_IN_SECONDS = "bookkeeper.client.health.check.quarantine.time.in.seconds";
    public static final String BOOKKEEPER_CLIENT_QUARANTINE_RATIO = "bookkeeper.client.quarantine.ratio";
    public static final String BOOKKEEPER_CLIENT_REORDER_READ_SEQUENCE_ENABLED = "bookkeeper.client.reorder.read.sequence.enabled";
    public static final String BOOKKEEPER_CLIENT_EXPLICIT_LAC_INTERVAL_IN_MILLS = "bookkeeper.client.explicit.lac.interval.in.mills";
    public static final String BOOKKEEPER_CLIENT_GET_BOOKIE_INFO_INTERVAL_SECONDS = "bookkeeper.client.get.bookie.info.interval.seconds";
    public static final String BOOKKEEPER_CLIENT_GET_BOOKIE_INFO_RETRY_INTERVAL_SECONDS = "bookkeeper.client.get.bookie.info.retry.interval.seconds";
    public static final String BOOKKEEPER_CLIENT_NUM_IO_THREADS = "bookkeeper.client.num.io.threads";
    public static final String BOOKKEEPER_CLIENT_LIMIT_STATS_LOGGING = "bookkeeper.client.limit.stats.logging";
    public static final String BOOKKEEPER_CLIENT_AUTHENTICATION_PARAMETERS_NAME = "bookkeeper.client.authentication.parameters.name";
}
