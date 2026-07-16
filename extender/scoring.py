"""
File:        scoring.py
Author:      Kevin Auberson
Created:     2026-06-03
Description: Computes carbon-aware priority scores (0–100) for candidate nodes
             using the formula:
               C_marginal = (1 + α × CI_norm) × P_node × (1 + w_cpu × CPU_load + w_mem × MEM_load)
             CPU and memory loads are weighted (default 70/30) to account for
             memory-intensive workloads that would otherwise be invisible.
             Scores are normalised using a mean-centred method so that small cost
             differences produce proportional scores rather than 0/100 extremes.
"""

import logging
import os

from workload_classifier import PENALTY_FACTORS, classify

log = logging.getLogger("scoring")

# Neutral score when all nodes are equivalent
NEUTRAL_SCORE = 50
MAX_SCORE = 100
MIN_SCORE = 0

# CPU/memory weighting in marginal cost.
W_CPU = float(os.getenv("SCORING_W_CPU", "0.7"))
W_MEM = float(os.getenv("SCORING_W_MEM", "0.3"))


class CarbonScorer:
    def __init__(self, signal_loader):
        self.signal_loader = signal_loader

    def score_nodes(self, pod: dict, node_names: list[str]) -> dict[str, int]:
        """Compute a 0-100 score for each candidate node. Returns {node_name: score}."""
        signal = self.signal_loader.load()
        if not signal:
            log.warning("No signal available, returning neutral scores")
            return {name: NEUTRAL_SCORE for name in node_names}

        carbon_class = classify(pod)
        alpha = PENALTY_FACTORS[carbon_class]
        ci = signal["grid_intensity_g_per_kwh"]
        ci_norm = self._normalize_ci(ci)

        log.info(
            f"Scoring pod {pod.get('metadata', {}).get('name', '?')} "
            f"class={carbon_class.value} α={alpha} CI={ci}gCO₂/kWh"
        )

        # 1. Compute marginal cost for each node
        marginal_costs = {}
        for name in node_names:
            cost = self._marginal_cost(name, signal, alpha, ci_norm)
            if cost is not None:
                marginal_costs[name] = cost

        if not marginal_costs:
            log.warning("No node data available, returning neutral scores")
            return {name: NEUTRAL_SCORE for name in node_names}

        # 2. Normalize to 0-100 scores
        return self._normalize_to_scores(marginal_costs, node_names)

    def _marginal_cost(
        self, node_name: str, signal: dict, alpha: float, ci_norm: float
    ) -> float | None:
        """
        C_marginal = (1 + α × CI_norm) × P_node × (1 + w_cpu × CPU_load + w_mem × MEM_load)
        """
        node = next((n for n in signal["nodes"] if n["name"] == node_name), None)
        if not node:
            log.warning(f"Node {node_name} not found in signal")
            return None

        p_node = node["watts"]
        cpu_load = self._estimate_cpu_load(node)
        mem_load = self._estimate_mem_load(node)
        combined_load = W_CPU * cpu_load + W_MEM * mem_load

        cost = (1 + alpha * ci_norm) * p_node * (1 + combined_load)

        log.debug(
            f"{node_name}: P={p_node:.2f}W, CPU={cpu_load:.2f}, MEM={mem_load:.2f}, "
            f"combined={combined_load:.2f}, α={alpha}, CI_norm={ci_norm:.2f} → C={cost:.4f}"
        )
        return cost

    def _estimate_cpu_load(self, node: dict) -> float:
        capacity = node.get("cpu_capacity_millicores")
        if capacity and capacity > 0:
            return min(node["cpu_millicores"] / capacity, 1.0)
        return min(node["cpu_millicores"] / 4000, 1.0)

    def _estimate_mem_load(self, node: dict) -> float:
        capacity = node.get("memory_capacity_mib")
        if capacity and capacity > 0:
            return min(node.get("memory_mib", 0) / capacity, 1.0)
        return min(node.get("memory_mib", 0) / 8192, 1.0)

    def _normalize_ci(self, ci: int) -> float:
        """Normalize grid intensity to [0, 1]. 500 gCO₂/kWh is the realistic upper bound."""
        return min(ci / 500.0, 1.0)

    def _normalize_to_scores(self, costs: dict[str, float], all_nodes: list[str]) -> dict[str, int]:
        """
        Normalize marginal costs to 0-100 scores using mean-centered normalization.

        Formula: score = clamp(50 + 50 × (c_mean - c_node) / c_mean, 0, 100)

        Advantages over classic min-max:
        - Proportional to actual spread: +5% cost → ~2-3 pts difference
        - Avoids systematic 0/100 with two nodes of similar cost
        - Preserves direction: lower cost → higher score
        """
        if not costs:
            return {name: NEUTRAL_SCORE for name in all_nodes}

        c_mean = sum(costs.values()) / len(costs)

        # Degenerate case: all nodes identical (e.g. vSphere unavailable)
        if c_mean < 1e-9:
            log.info("All nodes have near-zero cost, returning neutral score")
            return {name: NEUTRAL_SCORE for name in all_nodes}

        scores = {}
        for name in all_nodes:
            if name not in costs:
                scores[name] = NEUTRAL_SCORE
                continue
            raw = 50.0 + 50.0 * (c_mean - costs[name]) / c_mean
            scores[name] = int(round(max(MIN_SCORE, min(MAX_SCORE, raw))))

        return scores
