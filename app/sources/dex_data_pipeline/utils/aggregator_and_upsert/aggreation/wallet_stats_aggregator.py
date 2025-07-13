from collections import defaultdict
from decimal import Decimal

import logging
logger = logging.getLogger(__name__)

class WalletStatsAggregator:
    def __init__(self):
        self.wallets = defaultdict(lambda: {
            "volume_usd": Decimal(0),
            "pnl_usd": Decimal(0),
            "return_sum": Decimal(0),
            "return_squared_sum": Decimal(0),
            "num_returns": 0,
            "open_position_usd": Decimal(0),
            "cost_basis_usd": Decimal(0),
        })

    def add(self, log: dict):
        """
        Add a single decoded log (with usd_abs, usd_delta, is_buy).
        """
        w = log["sender"]
        usd_abs = Decimal(log["usd_abs"])
        usd_delta = Decimal(log["usd_delta"])
        is_buy = log["is_buy"]
        print(
        f"[agg-before] w={w[:6]}… "
        f"is_buy={is_buy} "
        f"usd_abs={usd_abs} "
        f"pos={self.wallets[w]['open_position_usd']} "
        f"cost={self.wallets[w]['cost_basis_usd']}"
        )
        if usd_abs < Decimal("0.01"):
            return

        state = self.wallets[w]

        # Always increment volume
        state["volume_usd"] += usd_abs

        if is_buy:
            # Buying base → spent USD → increase position
            state["open_position_usd"] += usd_abs
            state["cost_basis_usd"] += usd_abs
        else:
            # Selling base → received USD → realize PnL
            pos = state["open_position_usd"]
            if pos == 0:
                return  # can't sell what you don't have

            frac = usd_abs / pos
            cost_removed = state["cost_basis_usd"] * frac
            pnl = usd_abs - cost_removed

            if abs(pnl) < Decimal("0.0001"):
                logger.debug("[agg-clamp] w=%s raw_pnl=%s → 0", w[:6] + "…", pnl)
                pnl = Decimal(0)

            print(
                "[agg-sell] w=%s sell=%s frac=%s cost_removed=%s pnl=%s "
                "new_pos=%s new_cost=%s",
                w[:6] + "…", usd_abs, frac, cost_removed, pnl,
                state["open_position_usd"] - usd_abs,   # what it will become
                state["cost_basis_usd"] - cost_removed  # what it will become
            )
            ret_pct = pnl / cost_removed if cost_removed else Decimal(0)

            state["pnl_usd"] += pnl
            state["return_sum"] += ret_pct
            state["return_squared_sum"] += ret_pct * ret_pct
            state["num_returns"] += 1

            state["open_position_usd"] -= usd_abs
            state["cost_basis_usd"] -= cost_removed

    def results(self):
        """
        Get final per-wallet stats as a list of dicts ready for UPSERT.
        """
        out = []
        for wallet, stats in self.wallets.items():
            out.append({
                "wallet": wallet,
                **stats
            })
        return out