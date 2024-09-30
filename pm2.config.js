// this means if app restart {MAX_RESTART} times in 1 min then it stops
const NODE_ENV = process.env.NODE_ENV || 'development';

const MAX_RESTART = 10;
const MIN_UPTIME = 60000;

const settings = require('./config/settings.json');
const callback_worker_config = settings.callback_worker_config;

module.exports = {
  apps : [
    {
      name   : "core-api",
      script : `poetry run python -m snapshotter.gunicorn_core_launcher`,
      max_restarts: MAX_RESTART,
      min_uptime: MIN_UPTIME,
      error_file: "/dev/null",
      out_file: "/dev/null",
      env: {
        NODE_ENV: NODE_ENV,
        GUNICORN_WORKERS: 1,
      }
    },
    {
      name   : "auth-api",
      script : `poetry run python -m snapshotter.auth.gunicorn_auth_entry_launcher`,
      max_restarts: MAX_RESTART,
      min_uptime: MIN_UPTIME,
      error_file: "/dev/null",
      out_file: "/dev/null",
      env: {
        NODE_ENV: NODE_ENV,
        GUNICORN_WORKERS: 1,
      }
    },
    {
      name   : "health-manager",
      script : `poetry run python -m snapshotter.health_manager`,
      max_restarts: MAX_RESTART,
      min_uptime: MIN_UPTIME,
      error_file: "/dev/null",
      out_file: "/dev/null",
      env: {
        NODE_ENV: NODE_ENV,
      }
    },
    {
      name   : "processor-distributor",
      script : `poetry run python -m snapshotter.processor_distributor`,
      max_restarts: MAX_RESTART,
      min_uptime: MIN_UPTIME,
      error_file: "/dev/null",
      out_file: "/dev/null",
      env: {
        NODE_ENV: NODE_ENV,
      }
    },
    {
      name   : "system-event-detector",
      script : `poetry run python -m snapshotter.system_event_detector`,
      max_restarts: MAX_RESTART,
      min_uptime: MIN_UPTIME,
      error_file: "/dev/null",
      out_file: "/dev/null",
      env: {
        NODE_ENV: NODE_ENV,
      }
    },
    {
      name   : "delegate-worker",
      script : `poetry run python -m snapshotter.utils.delegate_worker`,
      instances: callback_worker_config.num_delegate_workers,
      max_restarts: MAX_RESTART,
      min_uptime: MIN_UPTIME,
      error_file: "/dev/null",
      out_file: "/dev/null",
      env: {
        NODE_ENV: NODE_ENV,
      }
    },
    {
      name   : "snapshot-worker",
      script : `poetry run python -m snapshotter.utils.snapshot_worker`,
      instances: callback_worker_config.num_snapshot_workers,
      max_restarts: MAX_RESTART,
      min_uptime: MIN_UPTIME,
      error_file: "/dev/null",
      out_file: "/dev/null",
      env: {
        NODE_ENV: NODE_ENV,
      }
    },
    {
      name   : "aggregation-worker",
      script : `poetry run python -m snapshotter.utils.aggregation_worker`,
      instances: callback_worker_config.num_aggregation_workers,
      max_restarts: MAX_RESTART,
      min_uptime: MIN_UPTIME,
      error_file: "/dev/null",
      out_file: "/dev/null",
      env: {
        NODE_ENV: NODE_ENV,
      }
    }
  ]
}
