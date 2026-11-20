# Purchase Order Source API

From this folder, copy `.env.example` to `.env` and fill in both Basic Auth values. For Coolify or Kubernetes, supply the same variables in the container environment.

```bash
docker build -t purchase-order-source .
docker run --rm --env-file .env -p 127.0.0.1:8000:8000 purchase-order-source
```

Open Swagger at [localhost:8000/docs](http://localhost:8000/docs). Use `GET /purchase-orders?load_type=initial` or `load_type=incremental` to read the bundled fixtures. The public health endpoint is `/health`.
