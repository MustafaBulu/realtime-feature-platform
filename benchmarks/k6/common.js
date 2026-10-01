export function config() {
  return {
    baseUrl: (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/$/, ''),
    entityPrefix: __ENV.ENTITY_PREFIX || 'bench-manual-measurement',
    entityCardinality: positiveInt(__ENV.ENTITY_CARDINALITY, 1000),
    distribution: (__ENV.DISTRIBUTION || 'UNIFORM').toUpperCase(),
    zipfSkew: positiveFloat(__ENV.ZIPF_SKEW, 1.1),
    benchmarkRunId: __ENV.BENCHMARK_RUN_ID || '',
    benchmarkPhase: __ENV.BENCHMARK_PHASE || 'measurement',
    featureNames: (__ENV.FEATURE_NAMES || 'request_count_total,entity_event_count_10m,entity_error_rate_10m,entity_avg_latency_ms_5m')
      .split(',')
      .map((value) => value.trim())
      .filter((value) => value.length > 0),
  };
}

export function featureName(settings) {
  return settings.featureNames[Math.floor(Math.random() * settings.featureNames.length)];
}

export function entityId(settings) {
  const index = settings.distribution === 'ZIPF'
    ? zipfIndex(settings.entityCardinality, settings.zipfSkew)
    : Math.floor(Math.random() * settings.entityCardinality);
  return `${settings.entityPrefix}-entity-${String(index).padStart(6, '0')}`;
}

export function featurePath(settings, prefix) {
  const entity = encodeURIComponent(entityId(settings));
  const feature = encodeURIComponent(featureName(settings));
  return `${settings.baseUrl}${prefix}/service/${entity}/${feature}`;
}

export function baselineQuery(settings) {
  const params = [];
  if (settings.benchmarkRunId.length > 0) {
    params.push(`benchmarkRunId=${encodeURIComponent(settings.benchmarkRunId)}`);
  }
  if (settings.benchmarkPhase.length > 0) {
    params.push(`benchmarkPhase=${encodeURIComponent(settings.benchmarkPhase)}`);
  }
  return params.length === 0 ? '' : `?${params.join('&')}`;
}

function zipfIndex(cardinality, skew) {
  let total = 0.0;
  for (let index = 1; index <= cardinality; index += 1) {
    total += 1.0 / Math.pow(index, skew);
  }
  let threshold = Math.random() * total;
  for (let index = 1; index <= cardinality; index += 1) {
    threshold -= 1.0 / Math.pow(index, skew);
    if (threshold <= 0) {
      return index - 1;
    }
  }
  return cardinality - 1;
}

function positiveInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function positiveFloat(value, fallback) {
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}
