import json
import base64
import gzip
import boto3
from collections import defaultdict
from datetime import datetime, timezone

s3 = boto3.client("s3")

# ── routing config ──
# Hello
CLIENT_BUCKETS = {
    "coimbra": "coimbra-production-logs",
    "yuma": "yuma-production-logs",
}

DEV_BUCKET = "tracelens-dev-logs"


def handler(event, context):
    groups = defaultdict(list)

    print("Received Firehose event")

    for record in event.get("records", []):
        record_id = record.get("recordId")

        try:
            compressed = base64.b64decode(record["data"])
            decompressed = gzip.decompress(compressed)
            envelope = json.loads(decompressed)
        except Exception as e:
            print(f"Failed to decode record {record_id}: {e}")
            continue

        # Skip CloudWatch control messages
        if envelope.get("messageType") != "DATA_MESSAGE":
            continue

        # This remembers which requestId belongs to which client/env
        request_routes = {}

        parsed_logs = []

        for log_event in envelope.get("logEvents", []):
            message = log_event.get("message", "")

            try:
                log = json.loads(message)

                # Remember route from your real application log
                request_id = log.get("requestId")
                env = log.get("environment", "dev")
                client = log.get("metadata", {}).get("client", "unknown")

                if request_id and client != "unknown":
                    request_routes[request_id] = {
                        "client": client,
                        "env": env
                    }

            except json.JSONDecodeError:
                # Convert START / END / REPORT / INIT_START into JSON
                request_id = extract_request_id(message)

                timestamp_ms = log_event.get("timestamp")

                if timestamp_ms:
                    timestamp = datetime.fromtimestamp(
                        timestamp_ms / 1000,
                        tz=timezone.utc
                    ).isoformat()
                else:
                    timestamp = datetime.now(timezone.utc).isoformat()

                log = {
                    "timestamp": timestamp,
                    "level": "DEBUG",
                    "environment": "dev",
                    "function": envelope.get("logGroup", "unknown").replace("/aws/lambda/", ""),
                    "requestId": request_id if request_id else log_event.get("id", "unknown"),
                    "message": message,
                    "metadata": {
                        "client": "unknown",
                        "source": "lambda-runtime",
                        "logGroup": envelope.get("logGroup"),
                        "logStream": envelope.get("logStream"),
                        "cloudWatchEventId": log_event.get("id")
                    }
                }

            parsed_logs.append(log)

        # Now route everything
        for log in parsed_logs:
            env = log.get("environment", "dev")
            client = log.get("metadata", {}).get("client", "unknown")
            request_id = log.get("requestId")

            # If this is a runtime log, try to route it using the app log requestId
            if log.get("metadata", {}).get("source") == "lambda-runtime":
                matched_route = request_routes.get(request_id)

                if matched_route:
                    client = matched_route["client"]
                    env = matched_route["env"]

                    log["environment"] = env
                    log["metadata"]["client"] = client
                    log["metadata"]["matchedByRequestId"] = True

            print(f"Routing log | client={client}, env={env}")

            # ── route to bucket ──
            if env == "dev":
                bucket = DEV_BUCKET
                prefix_client = client if client != "unknown" else "shared"

            elif env in ("prod", "production"):
                bucket = CLIENT_BUCKETS.get(client)

                if not bucket:
                    print(f"Unknown production client '{client}', routing to dev bucket")
                    bucket = DEV_BUCKET
                    prefix_client = client if client != "unknown" else "shared"
                    env = "dev"
                else:
                    prefix_client = client

            else:
                print(f"Unknown environment '{env}', routing to dev bucket")
                bucket = DEV_BUCKET
                prefix_client = client if client != "unknown" else "shared"
                env = "dev"

            groups[(bucket, prefix_client, env)].append(log)

    # ── write one gz per (bucket, client, env) group ──
    now = datetime.now(timezone.utc)
    date = now.strftime("%Y-%m-%d")

    for (bucket, client, env), records in groups.items():
        ndjson = "\n".join(json.dumps(r) for r in records)
        body = gzip.compress(ndjson.encode("utf-8"))

        # Production buckets already represent client + environment.
        if bucket != DEV_BUCKET:
            key = (
                f"date={date}/"
                f"{context.aws_request_id}.gz"
            )

        # Dev bucket is shared, so keep client/env folders to avoid mixing logs.
        else:
            key = (
                f"{client}/{env}/logs/"
                f"date={date}/"
                f"{context.aws_request_id}.gz"
            )

        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType="application/gzip",
            )

            print(f"Wrote {len(records)} records to s3://{bucket}/{key}")

        except Exception as e:
            print(f"Failed to write to s3://{bucket}/{key}: {e}")

    # ── return Firehose-compliant response ──
    return {
        "records": [
            {
                "recordId": record["recordId"],
                "result": "Ok",
                "data": record["data"],
            }
            for record in event.get("records", [])
        ]
    }


def extract_request_id(message):
    if not message:
        return None

    marker = "RequestId:"

    if marker in message:
        try:
            after_marker = message.split(marker, 1)[1].strip()
            return after_marker.split()[0]
        except Exception:
            return None

    return None