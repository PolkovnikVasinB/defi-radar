DeFi Radar
Live DeFi market data API for AI agents — pay per call with x402 v2 (USDC on Base). No API keys, no signups.

Base URL: https://radar.scv-agent.online

Endpoints
Endpoint	Price	What you get
GET /price?token=0x...	$0.003	Cross-chain token price with spread & liquidity ranking
GET /pools?token=0x...	$0.003	DEX pairs (DexScreener + GeckoTerminal), ranked by volume/liquidity
GET /yields?chain=base&max_apy=50	$0.003	16,000+ yield pools, APY filters, stable-only mode
GET /tvl	$0.002	Total TVL + breakdown by chains
GET /snapshot?token=0x...	$0.005	All-in-one brief: price, pools, yields, TVL
Payment (x402 v2)
Standard x402 flow: call any endpoint → get 402 with a PAYMENT-REQUIRED challenge → sign an EIP-3009 USDC transfer on Base → retry with PAYMENT-SIGNATURE header → receive data. Settlement by the PayAI facilitator.

Network: eip155:8453 (Base mainnet)
Asset: USDC 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913
Scheme: exact · Pay to: 0x868D008e4Bf33639510924f5bE9208acEb9a342d
Agent discovery
/.well-known/x402.json — service manifest
/openapi.json — OpenAPI 3 spec
/llms.txt — LLM-friendly overview
/health — live status
Free for humans
Dashboard: https://radar.scv-agent.online/
