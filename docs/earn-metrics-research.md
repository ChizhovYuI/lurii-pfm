# Earn Metrics Research: APR/APY for Earn & DeFi Accounts

## Summary

Research into how to obtain APR% and APY% for all earn/DeFi positions across the 9 data
sources. **4 sources** have active earn positions with yield data available via API.

| Source | Product | Asset | Effective APR | How to Get |
|--------|---------|-------|---------------|------------|
| OKX | Simple Earn + Bonus | USDT | **~10%** (bonus + market, additive) | `lending-history` API |
| OKX | Simple Earn + Bonus | USDC | **~10%** (bonus + market, additive) | `lending-history` API |
| OKX | Simple Earn + Bonus | ETH | **5.00%** (bonus, within limit) | `lending-history` API |
| OKX | Simple Earn + Bonus | BTC | **~5.00%** (bonus, within limit) | `lending-history` API |
| Bybit | Flexible Saving | USDT | **5.60%** (Tier 1 ≤200) | `yesterdayYield` from position |
| Bybit | Flexible Saving | USDC | **5.80%** (Tier 1 ≤200) | `yesterdayYield` from position |
| Bybit | On-Chain Staking | SOL | 4.38% | `estimateApr` from product |
| Blend | DeFi Lending (supply) | USDC | ~8.57% | On-chain calculation |
| Binance | Simple Earn (inactive) | — | — | `simple-earn/flexible/list` |

---

## 1. OKX Simple Earn (Savings) + 180-Day Bonus

### Active Positions

```json
{
  "USDT": { "amount": "501.48", "earnings": "0.508", "minRate": "1%" },
  "USDC": { "amount": "500.66", "earnings": "0.500", "minRate": "1%" },
  "ETH":  { "amount": "0.1999", "earnings": "0.000081", "minRate": "1%" },
  "BTC":  { "amount": "0.0100", "earnings": "0.000005", "minRate": "1%" }
}
```

### 180-Day Bonus Policy

OKX offers a one-time bonus APR for the first 180 days:

| Asset | Bonus APR | Individual Limit | Period |
|-------|-----------|------------------|--------|
| USDT | 10% | 1,000 USDT | 180 days |
| USDC | 10% | 1,000 USDC | 180 days |
| BTC | 5% | 0.01 BTC | 180 days |
| ETH | 5% | 0.2 ETH | 180 days |

- Within limit: earns bonus APR (or market APR if higher)
- Above limit: earns market APR only
- One-time per currency per account

### How to Get APR — The `lending-history` Approach

The **key finding** is that `lending-rate-summary` does NOT include the bonus rate. The
bonus is only visible in `lending-history`, where OKX splits each position into separate
entries per earning period: one for the bonus portion and one for the market portion.

**Endpoint:** `GET /api/v5/finance/savings/lending-history` (requires auth)

```
Params: ccy=USDT
Response (two entries per timestamp — bonus + market):
[
  { "amt": "500", "ccy": "USDT", "earnings": "0.00516438", "rate": "0.0904", "ts": "..." },
  { "amt": "501.46", "ccy": "USDT", "earnings": "0.00054496", "rate": "0.0112", "ts": "..." }
]
```

- `rate` — annual APR for this earning component (decimal, 0.0904 = 9.04%)
- `amt` — amount this component refers to
- `earnings` — earnings for this hourly period
- `ts` — timestamp of this earning event

**IMPORTANT:** The entries at each timestamp are **additive earnings, NOT separate portions
of the balance.** OKX splits earnings into two components:

1. **Bonus subsidy** (amt=500, rate=9.04%) — OKX pays the bonus rate on up to the limit
2. **Market lending** (amt=501.46, rate=1.12%) — actual lending income on the full balance

The effective APR = `sum(all earnings) * 8760 / balance`, giving ~10% total for USDT/USDC.

### Current Effective Rates (March 2026)

Effective APR = `sum(all hourly earnings) * 8760 / balance`

| Asset | Balance | Bonus Component | Market Component | **Effective APR** | **APY** |
|-------|---------|----------------|------------------|-------------------|---------|
| USDT | 501.48 | 500 @ 9.04% | 501.46 @ 1.12% | **9.97%** | 10.49% |
| USDC | 500.66 | 500 @ 9.20% | 500.65 @ 0.94% | **9.99%** | 10.50% |
| BTC | 0.01 | 0.01 @ 5.00% | — | **~5.00%** | 5.12% |
| ETH | 0.20 | 0.20 @ 5.00% | — | **5.00%** | 5.12% |

### Fallback: `lending-rate-summary`

For cases where bonus is expired or not applicable:

```
GET /api/v5/finance/savings/lending-rate-summary?ccy=USDT
Response: { "estRate": "0.025", "avgRate": "0.025", "preRate": "0.025" }
```

- `estRate` — estimated annual rate (decimal, 0.025 = 2.5% APR)
- Returns only the platform/market rate, **NOT** the bonus rate

### Implementation

```python
async def fetch_okx_earn_apr(self, ccy: str) -> tuple[Decimal, list[dict]]:
    """Get effective APR from lending history (includes bonus).

    Returns (effective_apr, components) where components is a list of
    {"amt": Decimal, "rate": Decimal, "earnings": Decimal}.
    """
    data = await self._get(
        "/api/v5/finance/savings/lending-history", {"ccy": ccy},
    )
    entries = data.get("data", [])
    if not entries:
        # Fallback to lending-rate-summary
        summary = await self._get(
            "/api/v5/finance/savings/lending-rate-summary", {"ccy": ccy},
        )
        apr = Decimal(summary["data"][0]["estRate"])
        return apr, []

    # Group by latest timestamp — entries are ADDITIVE earnings
    latest_ts = entries[0]["ts"]
    latest = [e for e in entries if e["ts"] == latest_ts]

    # Sum all earnings at this timestamp
    total_earnings = sum(Decimal(e["earnings"]) for e in latest)
    components = [
        {"amt": Decimal(e["amt"]), "rate": Decimal(e["rate"]),
         "earnings": Decimal(e["earnings"])}
        for e in latest
    ]

    # effective_apr = total_hourly_earnings * 8760 / balance
    effective_apr = total_earnings * 8760 / balance if balance > 0 else Decimal(0)
    return effective_apr, components
```

### Savings Balance Fields Reference

| Field | Description |
|-------|-------------|
| `amt` | Total amount in savings |
| `ccy` | Currency |
| `earnings` | Cumulative earnings |
| `loanAmt` | Amount currently lent out |
| `pendingAmt` | Amount pending (not yet lent) |
| `rate` | Minimum guaranteed annual rate (does NOT reflect bonus) |

### Lending History Fields Reference

| Field | Description |
|-------|-------------|
| `amt` | Amount for this earning portion |
| `ccy` | Currency |
| `earnings` | Earnings for this period |
| `rate` | **Actual annual APR** applied (includes bonus if applicable) |
| `ts` | Timestamp of earning event (hourly) |

---

## 2. Bybit Earn

### Active Positions

**Flexible Saving:**
- USDT: 200, totalPnl=0.1974, yesterdayYield=0.0307
- USDC: 179.70, totalPnl=0.0485, yesterdayYield=0.0286

**On-Chain Staking:**
- SOL: 7.891, status=Active (staking rewards not yet accumulated)

### How to Get APR

**Endpoint:** `GET /v5/earn/product` (requires auth)

```
Params: category=FlexibleSaving&coin=USDT
Response:
{
  "estimateApr": "0.6%",
  "hasTieredApr": true,
  "tierAprDetails": [
    { "min": "0", "max": "200", "estimateApr": "5.6%" },
    { "min": "200", "max": "-1", "estimateApr": "0.6%" }
  ]
}
```

### Current Rates (March 2026)

| Product | Asset | Tier 2 APR (>200) | Tier 1 APR (≤200) | **Real APR** | Verified via |
|---------|-------|-------------------|--------------------|--------------|----|
| FlexibleSaving | USDT | 0.6% | 5.6% | **5.60%** | `yesterdayYield` |
| FlexibleSaving | USDC | 0.8% | 5.8% | **5.80%** | `yesterdayYield` |
| OnChain | SOL | — | 4.38% | 4.38% | `estimateApr` |
| OnChain | SOL (BBSOL) | — | 6.10% | — | `estimateApr` |

**Important:** `estimateApr` from `/v5/earn/product` returns the **Tier 2 (base) rate** for
tiered products — NOT the rate the user actually earns. To get the real APR, use
`yesterdayYield` from `/v5/earn/position`:

```
real_apr = yesterdayYield * 365 / amount
```

For USDT: `0.030692 * 365 / 200 = 5.60%` — matches Tier 1 exactly.

### Position Fields Reference

| Field | Description |
|-------|-------------|
| `amount` | Total staked amount |
| `totalPnl` | Cumulative P&L from yield |
| `claimableYield` | Unclaimed yield |
| `yesterdayYield` | Yesterday's yield amount |
| `yesterdayYieldDate` | Date of yesterday's yield |
| `effectiveAmount` | Amount currently earning |
| `productId` | Links to product info |

### Implementation

```python
# Get APR for a product
data = await bybit_get("/v5/earn/product", {"category": category, "coin": coin})
product = data["result"]["list"][0]
apr_str = product["estimateApr"]  # e.g. "0.6%"
apr = Decimal(apr_str.rstrip("%")) / 100

# For tiered: compute effective APR based on position size
if product["hasTieredApr"]:
    for tier in product["tierAprDetails"]:
        tier_max = Decimal(tier["max"]) if tier["max"] != "-1" else Decimal("Infinity")
        tier_apr = Decimal(tier["estimateApr"].rstrip("%")) / 100
        # Apply tier logic based on position amount

# Compute APY
apy = (1 + apr / 365) ** 365 - 1
```

### Computing Effective Tiered APR

For positions spanning multiple tiers (e.g., 200 USDT in Bybit FlexibleSaving):

```python
def effective_apr(amount: Decimal, tiers: list[dict]) -> Decimal:
    """Weighted average APR across tiers."""
    remaining = amount
    weighted_sum = Decimal(0)
    for tier in tiers:
        tier_max = Decimal(tier["max"]) if tier["max"] != "-1" else remaining
        tier_apr = Decimal(tier["estimateApr"].rstrip("%")) / 100
        tier_amount = min(remaining, tier_max - Decimal(tier["min"]))
        weighted_sum += tier_amount * tier_apr
        remaining -= tier_amount
        if remaining <= 0:
            break
    return weighted_sum / amount
```

---

## 3. Blend Protocol (DeFi Lending on Stellar)

### Active Position

- USDC supply position via Soroban smart contract
- Pool contract: `CAJJZSGMMM3PD7N33TAPHGBUGTB43OC73HVIK2L2G6BNGGGYOSSYBXBD`

### How to Compute APR

Blend has **no API endpoint** for APR. It must be computed from on-chain reserve data using
the protocol's interest rate model.

#### Step 1: Get Reserve Data

```python
reserve = simulate("get_reserve", [asset_addr.to_xdr_sc_val()])
# Returns: { "data": { "b_rate", "d_rate", "b_supply", "d_supply", "ir_mod" },
#            "config": { "r_base", "r_one", "r_two", "r_three", "util", "max_util" },
#            "scalar": ... }
```

#### Step 2: Compute Utilization

```python
SCALAR_12 = 10**12
total_supply = b_supply * b_rate / SCALAR_12
total_borrow = d_supply * d_rate / SCALAR_12
utilization = total_borrow / total_supply
```

#### Step 3: Compute Borrow APR (3-segment piecewise curve)

All rate parameters are in 7-decimal fixed-point (10^7 = 100%).

```python
SCALAR_7 = 10**7

u = utilization
u_target = config["util"] / SCALAR_7     # e.g. 0.80
U_95 = 0.95

if u <= u_target:
    base_ir = (u / u_target) * r_one + r_base
elif u <= U_95:
    base_ir = ((u - u_target) / (U_95 - u_target)) * r_two + r_one + r_base
else:
    base_ir = ((u - U_95) / 0.05) * r_three + r_two + r_one + r_base

# Apply reactive interest rate modifier
cur_ir = base_ir * ir_mod / SCALAR_7

borrow_apr = cur_ir / SCALAR_7  # Convert to decimal
```

#### Step 4: Compute Supply APR

```python
backstop_take_rate = 0.20  # 20% — from pool config bstop_rate
supply_capture = (1 - backstop_take_rate) * utilization
supply_apr = borrow_apr * supply_capture
```

#### Step 5: Estimate APY

```python
# Blend SDK convention:
est_borrow_apy = (1 + borrow_apr / 365) ** 365 - 1
est_supply_apy = (1 + supply_apr / 52) ** 52 - 1  # weekly compounding
```

### Current Rates (March 2026)

| Asset | Utilization | Borrow APR | Supply APR | Supply APY | DefiLlama APY |
|-------|-------------|------------|------------|------------|---------------|
| USDC | 70.47% | 15.20% | 8.57% | 8.94% | 8.57% |
| EURC | 80.67% | 16.64% | 10.74% | 11.33% | 10.75% |
| XLM | 0.17% | 0.10% | 0.00% | 0.00% | 0.13% |

Verified against DefiLlama (`blend-pools-v2` on Stellar) — values match closely.

### Interest Rate Parameters

| Asset | r_base | r_one | r_two | r_three | target_util | max_util | ir_mod |
|-------|--------|-------|-------|---------|-------------|----------|--------|
| USDC | 300000 | 400000 | 1200000 | 50000000 | 80% | 90% | 2.33x |
| EURC | 300000 | 400000 | 1200000 | 50000000 | 80% | 90% | 2.21x |
| XLM | 100000 | 300000 | 3000000 | 50000000 | 40% | 70% | 0.10x |

Key: `ir_mod` is the reactive rate modifier that adjusts dynamically based on deviation
from target utilization. Pool `backstop_take_rate` = 20%.

### Getting Pool Config

```python
config = simulate("get_config", [])
# Returns: { "bstop_rate": 2000000, "max_positions": 6, ... }
backstop_rate = config["bstop_rate"] / 10**7  # 0.20 = 20%
```

---

## 4. Binance Simple Earn (No Active Positions)

No active earn positions, but the API supports querying available rates.

### Available Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/sapi/v1/simple-earn/flexible/list` | Product list with APR |
| `/sapi/v1/simple-earn/flexible/position` | Active flexible positions |
| `/sapi/v1/simple-earn/locked/position` | Active locked positions |

### Product Rate Data

```
GET /sapi/v1/simple-earn/flexible/list?asset=USDT
Response:
{
  "asset": "USDT",
  "latestAnnualPercentageRate": "0.00845837",  // 0.85% base
  "tierAnnualPercentageRate": {
    "0-200USDT": "0.03000000"   // 3% for first 200
  }
}
```

### Available Rates (for reference)

| Asset | Base APR | Tier 1 Rate | Tier 1 Limit |
|-------|----------|-------------|--------------|
| USDT | 0.85% | 3.00% | 200 USDT |
| USDC | 0.47% | 5.00% | 200 USDC |
| ETH | 1.72% | 0.10% | 0.2 ETH |
| ETC | 0.06% | — | — |

---

## 5. Sources Without Earn (No APR applicable)

| Source | Reason |
|--------|--------|
| Binance TH | Spot trading only, no earn products via API |
| Lobstr (Stellar) | Only holds XLM, no AMM/LP positions |
| Wise | Fiat multi-currency account, no yield product |
| KBank | Thai bank savings, interest rate not available via PDF |
| IBKR | Stocks/ETFs, dividend yield tracked separately |

---

## Implementation Plan

### Approach: Unified `EarnMetrics` per Position

```python
@dataclass
class EarnMetrics:
    source: str          # "okx", "bybit", "blend"
    asset: str           # "USDT", "SOL", etc.
    product_type: str    # "savings", "staking", "defi_lending"
    amount: Decimal      # current balance
    apr: Decimal         # annual percentage rate (decimal, e.g. 0.025 = 2.5%)
    apy: Decimal         # annual percentage yield
    daily_yield: Decimal # estimated daily yield in asset terms
    earnings: Decimal    # cumulative earnings (where available)
    tier_info: str | None  # "5.6% ≤200, 0.6% >200"
```

### Per-Source Implementation

#### OKX

```python
async def fetch_earn_metrics(self) -> list[EarnMetrics]:
    balances = await self._get("/api/v5/finance/savings/balance")
    metrics = []
    for item in balances["data"]:
        ccy = item["ccy"]
        amount = Decimal(item["amt"])

        # Use lending-history to get actual rate (includes bonus)
        effective_apr, portions = await self.fetch_okx_earn_apr(ccy)
        apy = (1 + effective_apr / 365) ** 365 - 1

        # Build tier info string from portions
        tier_info = None
        if len(portions) > 1:
            parts = [f"{p['rate']*100:.1f}% on {p['amt']:.0f}" for p in portions]
            tier_info = " + ".join(parts)

        metrics.append(EarnMetrics(
            source="okx", asset=ccy, product_type="savings",
            amount=amount, apr=effective_apr, apy=apy,
            daily_yield=amount * effective_apr / 365,
            earnings=Decimal(item["earnings"]),
            tier_info=tier_info,
        ))
    return metrics
```

#### Bybit

```python
async def fetch_earn_metrics(self) -> list[EarnMetrics]:
    metrics = []
    for category in ("FlexibleSaving", "OnChain"):
        positions = await self._get("/v5/earn/position", {"category": category})
        for pos in positions["result"]["list"]:
            coin = pos["coin"]
            product = await self._get("/v5/earn/product", {"category": category, "coin": coin})
            prod_info = product["result"]["list"][0]
            apr = Decimal(prod_info["estimateApr"].rstrip("%")) / 100
            amount = Decimal(pos["amount"])
            # Compute effective tiered APR if applicable
            if prod_info.get("hasTieredApr"):
                apr = compute_tiered_apr(amount, prod_info["tierAprDetails"])
            apy = (1 + apr / 365) ** 365 - 1
            metrics.append(EarnMetrics(
                source="bybit", asset=coin,
                product_type="staking" if category == "OnChain" else "savings",
                amount=amount, apr=apr, apy=apy,
                daily_yield=amount * apr / 365,
                earnings=Decimal(pos.get("totalPnl", "0")),
            ))
    return metrics
```

#### Blend

```python
async def fetch_earn_metrics(self) -> list[EarnMetrics]:
    # Requires on-chain computation (see Section 3)
    reserve_list = self._get_reserve_list()
    positions = self._get_positions()
    pool_config = self._simulate("get_config", [])
    backstop_rate = int(pool_config["bstop_rate"]) / SCALAR_7

    metrics = []
    for idx, b_tokens in merged_positions.items():
        reserve = self._get_reserve(reserve_list[idx])
        utilization = compute_utilization(reserve)
        borrow_apr = compute_borrow_apr(reserve, utilization)
        supply_apr = borrow_apr * (1 - backstop_rate) * utilization
        supply_apy = (1 + supply_apr / 52) ** 52 - 1
        amount = convert_btokens_to_underlying(b_tokens, reserve)
        metrics.append(EarnMetrics(
            source="blend", asset=ticker, product_type="defi_lending",
            amount=amount, apr=supply_apr, apy=supply_apy,
            daily_yield=amount * supply_apr / 365,
            earnings=Decimal(0),  # No cumulative earnings from Blend
        ))
    return metrics
```

### API Endpoint Design

```
GET /api/v1/analytics/earn-metrics
Response:
{
  "positions": [
    {
      "source": "okx",
      "asset": "USDT",
      "product_type": "savings",
      "amount": 501.48,
      "usd_value": 501.48,
      "apr_pct": 2.50,
      "apy_pct": 2.53,
      "daily_yield": 0.0343,
      "daily_yield_usd": 0.0343,
      "earnings": 0.508,
      "tier_info": null
    },
    ...
  ],
  "totals": {
    "total_earning_usd": 2250.00,
    "weighted_avg_apy_pct": 4.12,
    "estimated_daily_yield_usd": 0.25,
    "estimated_monthly_yield_usd": 7.60,
    "estimated_annual_yield_usd": 92.70
  }
}
```

### Key Considerations

1. **Rate freshness**: OKX/Bybit rates change frequently. Cache for 1h max.
2. **Blend rates are live**: Computed from on-chain state, no caching needed during request.
3. **Tiered APR**: Bybit uses tiered rates — compute effective rate based on position size.
4. **APR vs APY**: APR is the simple rate. APY accounts for compounding:
   - OKX/Bybit: daily compounding → `APY = (1 + APR/365)^365 - 1`
   - Blend: weekly compounding per SDK convention → `APY = (1 + APR/52)^52 - 1`
5. **Blend backstop**: 20% of interest goes to backstop module, not suppliers.
6. **No historical APR storage needed**: Always fetch live rates per request.

### API Calls Required per Refresh

| Source | Calls | Auth | Notes |
|--------|-------|------|-------|
| OKX | 1 (balance) + N (rate per ccy) | Yes | N = number of savings currencies |
| Bybit | 2 (positions) + N (products) | Yes | N = number of earn products held |
| Blend | 3-5 Soroban simulations | No auth | reserve_list + positions + config + N reserves |
| Binance | 1 (positions) | Yes | Only if positions exist |
