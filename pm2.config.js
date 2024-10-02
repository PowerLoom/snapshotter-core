const NODE_ENV = 'development';
const MIN_UPTIME = 60000; // 1 minute in milliseconds
const settings = require('./config/settings.json');
const { callback_worker_config } = settings;

// Common configuration for all apps
const commonConfig = {
  min_uptime: MIN_UPTIME,
  error_file: "/dev/null",
  out_file: "/dev/null",
  env: {
    NODE_ENV: NODE_ENV,
  },
};

// Helper function to create app configurations
const createAppConfig = (name, script, additionalConfig = {}) => ({
  name,
  script: `poetry run python -m ${script}`,
  ...commonConfig,
  ...additionalConfig,
});

// Helper function to create worker configurations
const createWorkerConfig = (name, script, instances, cronRestart) => {
  if (instances === 0) {
    return null; // Return null for zero count services
  }
  return createAppConfig(name, script, {
    instances,
    cron_restart: cronRestart,
  });
};

// Create app configurations
const appConfigs = [
  createAppConfig("core-api", "snapshotter.gunicorn_core_launcher", {
    env: {
      ...commonConfig.env,
      GUNICORN_WORKERS: 1,
    },
  }),
  createAppConfig("auth-api", "snapshotter.auth.gunicorn_auth_entry_launcher", {
    env: {
      ...commonConfig.env,
      GUNICORN_WORKERS: 1,
    },
  }),
  createAppConfig("processor-distributor", "snapshotter.processor_distributor", {
    cron_restart: '0 0 * * *', // Every day at 3 AM
  }),
  createAppConfig("system-event-detector", "snapshotter.system_event_detector"),
  createWorkerConfig("delegate-worker", "snapshotter.utils.delegate_worker",
    callback_worker_config.num_delegate_workers, '0 */6 * * *'),
  createWorkerConfig("snapshot-worker", "snapshotter.utils.snapshot_worker",
    callback_worker_config.num_snapshot_workers, '0 */6 * * *'),
  createWorkerConfig("aggregation-worker", "snapshotter.utils.aggregation_worker",
    callback_worker_config.num_aggregation_workers, '0 */6 * * *'),
];

// Filter out null configurations (zero count services)
const validAppConfigs = appConfigs.filter(config => config !== null);

module.exports = {
  apps: validAppConfigs,
};
