# Rate Limiter Service

A lightweight rate limiting service that supports multiple rate limits for different keys and tracks usage statistics.

## Features

- **Multiple Rate Limits**: Configure different rate limits for different keys
- **Statistics Tracking**: Track hourly and daily usage for each key
- **In-Memory Storage**: Simple in-memory storage for both rate limits and statistics
- **Health Checks**: Endpoint to verify service health

## API Endpoints

### Check Rate Limit

```
GET /check/{key}
```

Checks if a key is within its rate limit. Returns:
- `200 OK` if within rate limit
- `429 Too Many Requests` if rate limit exceeded

Example response:
```json
{
  "status": "ok",
  "key": "example-key",
  "rate_limit": "10/second",
  "timestamp": "2023-05-01T12:34:56.789012"
}
```

### Configure Rate Limit

```
POST /configure
```

Configure a custom rate limit for a key.

Request body:
```json
{
  "key": "example-key",
  "limit": "100/minute"
}
```

Response:
```json
{
  "status": "ok",
  "key": "example-key",
  "rate_limit": "100/minute"
}
```

### Get Statistics

```
GET /stats/{key}
```

Get hourly and daily usage statistics for a key.

Example response:
```json
{
  "key": "example-key",
  "hourly_calls": {
    "2023-05-01-12": 45,
    "2023-05-01-11": 23
  },
  "daily_calls": {
    "2023-05-01": 152,
    "2023-04-30": 89
  },
  "current_rate_limit": "100/minute"
}
```

### Health Check

```
GET /health
```

Check the health of the service.

Example response:
```json
{
  "status": "ok",
  "timestamp": "2023-05-01T12:34:56.789012"
}
```

## Environment Variables

- `DEFAULT_RATE_LIMIT`: Default rate limit (e.g., "10/second")
- `PORT`: Port to run the service on (default: 8000)

## Running the Service

### Using Docker

```bash
docker build -t rate-limiter .
docker run -p 8000:8000 rate-limiter
```

### Using Python

```bash
pip install -r requirements.txt
python app.py
```

## Rate Limit Format

Rate limits should be specified in the format `{number}/{unit}` where:
- `number`: A positive integer
- `unit`: One of "second", "minute", "hour", "day"

Examples:
- "10/second"
- "100/minute"
- "1000/hour"
- "5000/day"

## Important Notes

This service uses in-memory storage, which means all rate limits and statistics will be lost when the service restarts. This is suitable for development and testing environments, but for production use cases requiring persistence, consider implementing a database backend.
