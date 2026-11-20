# Purchase Order Target API

From this folder, copy `.env.example` to `.env` and fill in the Basic Auth and MySQL settings. For Coolify or Kubernetes, supply the same variables in the container environment.

The MySQL database must already exist and be reachable from the container. At startup the API creates any missing `purchase_orders` and `purchase_order_events` tables if the MySQL user has `CREATE` permission. A setup account can also apply `schema.sql` and `events_schema.sql` first. Give the runtime account `SELECT`, `INSERT`, and `UPDATE` on both tables.

```bash
docker build -t purchase-order-target .
docker run --rm --env-file .env -p 127.0.0.1:8001:8001 purchase-order-target
```

Open Swagger at [localhost:8001/docs](http://localhost:8001/docs). Send a single order with `OrderNumber` to `POST /purchase-orders`. All successful requests return HTTP 200 with a status of `inserted`, `updated`, or `duplicate`. The public `/health` endpoint checks the MySQL table.

For pipeline requests, pass the 64-character event ID as `Idempotency-Key` and include a timezone-aware `LastUpdated` in the order. The API also returns `stale` when a newer snapshot is already stored. Reusing a key for a different payload returns HTTP 409.
