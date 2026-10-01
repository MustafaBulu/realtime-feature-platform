import http from 'k6/http';
import { check } from 'k6';
import { baselineQuery, config, featurePath } from './common.js';

export const options = {
  scenarios: {
    baseline_reads: {
      executor: 'constant-arrival-rate',
      rate: Number.parseInt(__ENV.RATE || '20', 10),
      timeUnit: '1s',
      duration: __ENV.DURATION || '1m',
      preAllocatedVUs: Number.parseInt(__ENV.VUS || '20', 10),
      maxVUs: Number.parseInt(__ENV.MAX_VUS || '100', 10),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
  },
};

const settings = config();

export default function () {
  const response = http.get(`${featurePath(settings, '/baseline/features')}${baselineQuery(settings)}`);
  check(response, {
    'status is 2xx': (res) => res.status >= 200 && res.status < 300,
  });
}
