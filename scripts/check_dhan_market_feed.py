"""
Dhan 20-Level Market Depth / Tick-Data Access Check
=====================================================
Answers one question with a definitive yes/no, instead of guessing from
a settings page: does this Dhan account's API access actually include
the 20-level market depth + tick-data WebSocket feed needed to build
real Delta/CVD/footprint for the V2 order-flow scanner (order_flow.py)?

WHY A SCRIPT INSTEAD OF A DASHBOARD CHECK: a WebSocket connection either
gets real depth packets back or Dhan rejects/closes it — there's no
ambiguity to misread, unlike scanning a settings page for a checkbox
that may or may not exist under the current plan/UI.

RUN THIS YOURSELF, NOT IN A SHARED/REMOTE SESSION — it needs your live
Dhan API token, which should never be pasted into a chat or committed
to git.

Setup:
    pip install websockets
    export DHAN_ACCESS_TOKEN="..."   # same token api.py already uses
    export DHAN_CLIENT_ID="..."      # your Dhan client ID (numeric)
    python scripts/check_dhan_market_feed.py [SECURITY_ID] [EXCHANGE_SEGMENT]

    Defaults to RELIANCE on NSE_EQ if no args given — VERIFY that
    Security ID against Dhan's own instrument/scrip master first
    (Dhan support or your Dhan API dashboard can give you the current
    CSV); the value below is filled in from public documentation
    examples and may not be current.

Reference (DhanHQ v2 docs — verify against the live docs before
trusting this over what Dhan actually publishes, protocol details do
change): 20-level depth endpoint is
wss://depth-api-feed.dhan.co/twentydepth?version=2&token=...&clientId=...&authType=2,
subscribe via a JSON RequestCode message. See
https://dhanhq.co/docs/v2/full-market-depth/ and
https://dhanhq.co/docs/v2/live-market-feed/ for the authoritative,
up-to-date spec.
"""

import asyncio
import json
import os
import sys

try:
    import websockets
except ImportError:
    print("Missing dependency — run: pip install websockets")
    sys.exit(1)

DEPTH_WS_BASE = "wss://depth-api-feed.dhan.co/twentydepth"
DEFAULT_SECURITY_ID = "2885"          # RELIANCE on NSE_EQ, per public Dhan examples — VERIFY before trusting
DEFAULT_SEGMENT      = "NSE_EQ"
LISTEN_SECONDS        = 10


async def check_depth_access(security_id, segment):
    token     = os.environ.get("DHAN_ACCESS_TOKEN", "")
    client_id = os.environ.get("DHAN_CLIENT_ID", "")
    if not token or not client_id:
        print("Set DHAN_ACCESS_TOKEN and DHAN_CLIENT_ID in your environment first.")
        return

    url = f"{DEPTH_WS_BASE}?version=2&token={token}&clientId={client_id}&authType=2"
    print(f"Connecting to 20-level depth feed for {segment}:{security_id} ...")

    try:
        async with websockets.connect(url, open_timeout=15) as ws:
            print("WebSocket handshake succeeded — connection was accepted.")

            subscribe_msg = {
                "RequestCode": 15,
                "InstrumentCount": 1,
                "InstrumentList": [
                    {"ExchangeSegment": segment, "SecurityId": str(security_id)}
                ],
            }
            await ws.send(json.dumps(subscribe_msg))
            print(f"Sent subscribe request: {subscribe_msg}")

            got_binary_packet = False
            got_error_text    = False
            try:
                async with asyncio.timeout(LISTEN_SECONDS):
                    while True:
                        msg = await ws.recv()
                        if isinstance(msg, (bytes, bytearray)):
                            got_binary_packet = True
                            print(f"Received BINARY packet — {len(msg)} bytes. "
                                  "This is a real depth/tick packet: access CONFIRMED.")
                            break
                        else:
                            got_error_text = True
                            print(f"Received TEXT message (usually an error/status frame): {msg}")
            except (asyncio.TimeoutError, TimeoutError):
                pass

            print()
            if got_binary_packet:
                print("VERDICT: 20-level depth + tick access is WORKING on this account. "
                      "Safe to build V2 order-flow ingestion directly on this feed.")
            elif got_error_text:
                print("VERDICT: connection accepted but Dhan sent a text/error frame instead of "
                      "binary depth data — likely means this plan/token does NOT include depth "
                      "access. Read the message above; if it names a permission/plan issue, that's "
                      "the email-to-Dhan-support ammunition.")
            else:
                print(f"VERDICT: connected and subscribed, but no packets arrived in "
                      f"{LISTEN_SECONDS}s. Could mean: market is closed right now (data only "
                      "flows during NSE trading hours), the Security ID is wrong, or access "
                      "genuinely isn't enabled. Re-run this during market hours (9:15am-3:30pm "
                      "IST, Mon-Fri) before concluding it's a plan/access problem.")

    except websockets.exceptions.InvalidStatusCode as e:
        print(f"VERDICT: WebSocket handshake was REJECTED — HTTP {e.status_code}. "
              "This is the clearest possible signal that depth access is not enabled on this "
              "token/account. Forward this exact output to Dhan support.")
    except Exception as e:
        print(f"Connection failed: {type(e).__name__}: {e}")
        print("VERDICT: inconclusive — could be a network issue, wrong token, or wrong URL. "
              "Double check DHAN_ACCESS_TOKEN/DHAN_CLIENT_ID and re-run.")


if __name__ == "__main__":
    sec_id  = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SECURITY_ID
    segment = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_SEGMENT
    asyncio.run(check_depth_access(sec_id, segment))
