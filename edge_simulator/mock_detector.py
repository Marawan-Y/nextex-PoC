"""
edge_simulator.mock_detector
===============================
Simulates the output of the real Jetson-side anomaly detection model
referenced in the assignment ("a mocked anomaly detection result for each
frame"). Rather than pure random noise, this generates *plausible* model
behavior:

  - When the frame's ground-truth label (from the synthetic generator, or
    the folder name if a labeled Kaggle set is used) is "no_defect", the
    mock model outputs "no_defect" with high confidence most of the time,
    with occasional low-confidence false positives — mimicking a real
    model's noise floor.
  - When the frame has a genuine defect, the mock model gets the class
    right most of the time with a confidence drawn from a realistic
    distribution, and occasionally misclassifies or reports lower
    confidence — mimicking real-world model uncertainty rather than a
    suspiciously perfect oracle.

This keeps the demo honest: the UI's alert and "new class" logic is being
exercised against realistic-looking confidence traces, not against a
detector that is either always right or pure noise.
"""
from __future__ import annotations

import random
from dataclasses import dataclass

from .dataset import DEFECT_CLASSES


@dataclass
class DetectionResult:
    anomaly_class: str
    confidence: float


class MockDetector:
    def __init__(self, seed: int = 7):
        self._rng = random.Random(seed)

    def detect(self, ground_truth_label: str) -> DetectionResult:
        label = ground_truth_label if ground_truth_label in DEFECT_CLASSES else "no_defect"

        if label == "no_defect":
            if self._rng.random() < 0.03:
                # rare low-confidence false positive, realistic noise floor
                false_class = self._rng.choice([c for c in DEFECT_CLASSES if c != "no_defect"])
                return DetectionResult(anomaly_class=false_class, confidence=round(self._rng.uniform(0.4, 0.65), 3))
            return DetectionResult(anomaly_class="no_defect", confidence=round(self._rng.uniform(0.9, 0.995), 3))

        # genuine defect frame
        if self._rng.random() < 0.9:
            confidence = round(self._rng.uniform(0.72, 0.98), 3)
            return DetectionResult(anomaly_class=label, confidence=confidence)
        else:
            # occasional misclassification / lower-confidence miss
            other = self._rng.choice([c for c in DEFECT_CLASSES if c != label])
            confidence = round(self._rng.uniform(0.55, 0.8), 3)
            return DetectionResult(anomaly_class=other, confidence=confidence)
