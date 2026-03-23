import asyncio
import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException


ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:5000")
CHECKOUT_TIMEOUT_SECONDS = float(os.getenv("CHECKOUT_TIMEOUT_SECONDS", "35"))
CHECKOUT_POLL_INTERVAL = float(os.getenv("CHECKOUT_POLL_INTERVAL", "0.25"))

app = FastAPI(title="order-checkout-worker")


def _map_final_status(status_payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    status = status_payload.get("status")
    if status == "completed":
        return 200, {
            "status": "success",
            "order_id": status_payload.get("order_id"),
            "correlation_id": status_payload.get("correlation_id"),
            "message": "Checkout completed successfully.",
            "results": status_payload.get("results", {}),
        }
    if status in {"failed", "compensated"}:
        return 400, {
            "status": "failed",
            "order_id": status_payload.get("order_id"),
            "correlation_id": status_payload.get("correlation_id"),
            "message": "Checkout failed.",
            "results": status_payload.get("results", {}),
        }
    return 500, {
        "status": "failed",
        "message": f"Unexpected final saga status: {status}",
    }


@app.post("/orders/checkout/{order_id}")
async def checkout(order_id: str):
    timeout = httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=10.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        start_resp = await client.post(f"{ORDER_SERVICE_URL}/checkout/start/{order_id}")
        if start_resp.status_code >= 400:
            raise HTTPException(start_resp.status_code, detail=start_resp.text)

        start_payload = start_resp.json()
        correlation_id = start_payload.get("correlation_id")
        if not correlation_id:
            raise HTTPException(500, detail="Order service did not return correlation_id")

        deadline = asyncio.get_running_loop().time() + CHECKOUT_TIMEOUT_SECONDS
        while True:
            if asyncio.get_running_loop().time() >= deadline:
                raise HTTPException(
                    504,
                    detail={
                        "status": "timeout",
                        "order_id": order_id,
                        "correlation_id": correlation_id,
                        "message": "Checkout timed out waiting for final saga result.",
                    },
                )

            status_resp = await client.get(f"{ORDER_SERVICE_URL}/checkout/status/{correlation_id}")
            if status_resp.status_code == 404:
                await asyncio.sleep(CHECKOUT_POLL_INTERVAL)
                continue
            if status_resp.status_code >= 400:
                raise HTTPException(status_resp.status_code, detail=status_resp.text)

            status_payload = status_resp.json()
            if status_payload.get("status") in {"completed", "failed", "compensated"}:
                code, body = _map_final_status(status_payload)
                if code >= 400:
                    raise HTTPException(code, detail=body)
                return body

            await asyncio.sleep(CHECKOUT_POLL_INTERVAL)
