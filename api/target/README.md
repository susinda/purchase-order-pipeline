# Purchase Order Target API

From this folder, copy `.env.example` to `.env` and fill in the Basic Auth and MySQL settings. For Coolify or Kubernetes, supply the same variables in the container environment.

The MySQL database must already exist and be reachable from the container. The API creates the `purchase_orders` table at startup if it is missing and the MySQL user has `CREATE` permission. You can also apply `schema.sql` with a setup account first.

```bash
docker build -t purchase-order-target .
docker run --rm --env-file .env -p 127.0.0.1:8001:8001 purchase-order-target
```

Open Swagger at [localhost:8001/docs](http://localhost:8001/docs). Send a single order with `OrderNumber` to `POST /purchase-orders`. All successful requests return HTTP 200 with a status of `inserted`, `updated`, or `duplicate`. The public `/health` endpoint checks the MySQL table.
