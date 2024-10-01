const NODE_ENV = process.env.NODE_ENV || 'development';
const MIN_UPTIME = 60000; // 1 minute in milliseconds
const settings = require('./config/settings.json');
const { callback_worker_config } = settings;

// Common configuration for all apps
const commonConfig = {
  min_uptime: MIN_UPTIME,
  error_file: process.env.PM2_ERROR_LOG || "/dev/null",
  out_file: process.env.PM2_OUT_LOG || "/dev/null",
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

module.exports = {
  apps: [
    createAppConfig("core-api", "snapshotter.gunicorn_core_launcher", {
      env: {
        ...commonConfig.env,
        GUNICORN_WORKERS: process.env.CORE_API_WORKERS || 1,
      },
    }),
    createAppConfig("auth-api", "snapshotter.auth.gunicorn_auth_entry_launcher", {
      env: {
        ...commonConfig.env,
        GUNICORN_WORKERS: process.env.AUTH_API_WORKERS || 1,
      },
    }),
    createAppConfig("health-manager", "snapshotter.health_manager", {
      cron_restart: '0 0 * * *', // Every day at midnight
    }),
    createAppConfig("processor-distributor", "snapshotter.processor_distributor", {
      cron_restart: '0 3 * * *', // Every day at 3 AM
    }),
    createAppConfig("system-event-detector", "snapshotter.system_event_detector"),
    createAppConfig("delegate-worker", "snapshotter.utils.delegate_worker", {
      instances: process.env.DELEGATE_WORKERS || callback_worker_config.num_delegate_workers,
      cron_restart: '0 6 * * *', // Every day at 6 AM
    }),
    createAppConfig("snapshot-worker", "snapshotter.utils.snapshot_worker", {
      instances: process.env.SNAPSHOT_WORKERS || callback_worker_config.num_snapshot_workers,
      cron_restart: '0 9 * * *', // Every day at 9 AM
    }),
    createAppConfig("aggregation-worker", "snapshotter.utils.aggregation_worker", {
      instances: process.env.AGGREGATION_WORKERS || callback_worker_config.num_aggregation_workers,
      cron_restart: '0 18 * * *', // Every day at 6 PM
    }),
  ],
};
