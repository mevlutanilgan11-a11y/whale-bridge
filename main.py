import asyncio
import websockets
import json
import os
import logging
import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

WHALE_ALERT_API_KEY = os.environ.get("WHALE_ALERT_API_KEY", "")
N8N_WEBHOOK_URL     = os.environ.get("N8N_WEBHOOK_URL", "")
WEBSOCKET_URL       = "wss://lexi.whale-alert.io/v1/transactions"


async def forward_to_n8n(session: aiohttp.ClientSession, data: dict):
    """Gelen veriyi olduğu gibi n8n webhook'una ilet."""
    try:
        async with session.post(
            N8N_WEBHOOK_URL,
            json=data,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            logger.info(f"→ n8n'e iletildi | HTTP {resp.status}")
    except Exception as e:
        logger.error(f"n8n iletim hatası: {e}")


async def connect_and_bridge():
    headers = {"X-WA-API-KEY": WHALE_ALERT_API_KEY}

    async with aiohttp.ClientSession() as http_session:
        while True:
            try:
                logger.info("🔌 Whale Alert WebSocket'e bağlanılıyor...")
                async with websockets.connect(
                    WEBSOCKET_URL,
                    additional_headers=headers,
                    ping_interval=30,
                    ping_timeout=10,
                ) as ws:
                    logger.info("✅ Bağlandı! Veriler n8n'e iletiliyor...")

                    await ws.send(json.dumps({
                        "type": "subscribe_transactions",
                        "api_key": WHALE_ALERT_API_KEY,
                    }))

                    async for raw in ws:
                        try:
                            data = json.loads(raw)

                            # Ping/pong yönet, gerisini n8n'e gönder
                            if data.get("type") == "ping":
                                await ws.send(json.dumps({"type": "pong"}))
                                continue

                            logger.info(f"📨 Veri alındı: {data.get('type','?')} | n8n'e iletiliyor...")
                            await forward_to_n8n(http_session, data)

                        except json.JSONDecodeError:
                            logger.warning(f"JSON parse hatası: {raw[:100]}")

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"🔴 Bağlantı kesildi: {e} — 5sn sonra yeniden bağlanıyor...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Hata: {e} — 10sn sonra yeniden deneniyor...")
                await asyncio.sleep(10)


if __name__ == "__main__":
    if not all([WHALE_ALERT_API_KEY, N8N_WEBHOOK_URL]):
        logger.error("❌ WHALE_ALERT_API_KEY ve N8N_WEBHOOK_URL gerekli!")
        exit(1)

    logger.info(f"🚀 Köprü başlatıldı | n8n: {N8N_WEBHOOK_URL}")
    asyncio.run(connect_and_bridge())
