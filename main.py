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


async def forward_to_n8n(session: aiohttp.ClientSession, data: dict):
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
    url = f"wss://leviathan.whale-alert.io/ws?api_key={WHALE_ALERT_API_KEY}"
    retry_delay = 30

    async with aiohttp.ClientSession() as http_session:
        while True:
            try:
                logger.info("🔌 Whale Alert WebSocket'e bağlanılıyor...")
                async with websockets.connect(url, ping_interval=30, ping_timeout=10) as ws:
                    logger.info("✅ Bağlandı! Alert'lere subscribe olunuyor...")
                    retry_delay = 30

                    await ws.send(json.dumps({
                        "type": "subscribe_alerts",
                        "min_value_usd": 100000
                    }))

                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                            msg_type = data.get("type", "")

                            if msg_type == "subscribed_alerts":
                                logger.info(f"✅ Subscribe başarılı! Channel ID: {data.get('channel_id')}")
                                continue

                            if msg_type == "alert":
                                logger.info(f"📨 Alert: {data.get('text', '')[:80]}...")
                                await forward_to_n8n(http_session, data)
                                continue

                            logger.info(f"📩 Mesaj alındı (type: {msg_type})")

                        except json.JSONDecodeError:
                            logger.warning(f"JSON parse hatası: {raw[:100]}")

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"🔴 Bağlantı kesildi: {e} — {retry_delay}sn sonra yeniden bağlanıyor...")
                await asyncio.sleep(retry_delay)

            except Exception as e:
                error_str = str(e)
                if "429" in error_str:
                    logger.warning(f"⏳ Rate limit (429) — {retry_delay}sn bekleniyor...")
                    retry_delay = min(retry_delay * 2, 300)
                else:
                    logger.error(f"Hata: {e} — {retry_delay}sn sonra yeniden deneniyor...")
                await asyncio.sleep(retry_delay)


if __name__ == "__main__":
    if not all([WHALE_ALERT_API_KEY, N8N_WEBHOOK_URL]):
        logger.error("❌ WHALE_ALERT_API_KEY ve N8N_WEBHOOK_URL gerekli!")
        exit(1)

    logger.info(f"🚀 Köprü başlatıldı | n8n: {N8N_WEBHOOK_URL}")
    asyncio.run(connect_and_bridge())
