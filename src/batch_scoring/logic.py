"""Pure decisions. No SDK types, no clients, no context.

Everything here is callable from a plain pytest test with plain dicts.
"""

from collections.abc import Sequence

REQUIRED_FEATURE_FIELDS = (
    'applicationId',
    'annualIncome',
    'creditScore',
    'debtToIncome',
    'employmentMonths',
)
RISK_BANDS = ('approve', 'refer', 'decline')
DECLINE_THRESHOLD = 0.66
REFER_THRESHOLD = 0.33
UNIDENTIFIED_APPLICATION = 'unidentified'


class ScoringRunAborted(Exception):
    """Too many batches failed for the run's scores to be published as complete."""


def group_into_batches(application_ids: Sequence[str], max_items_per_batch: int) -> list[list[str]]:
    """Split the run's applications into the batches one map iteration each will score.

    `MapConfig.item_batcher` is inert in SDK 1.7.0, so the grouping the map
    operation appears to offer has to happen before the inputs reach it.
    """
    if max_items_per_batch < 1:
        raise ValueError(f'max_items_per_batch must be at least 1, got {max_items_per_batch}')
    return [
        list(application_ids[start : start + max_items_per_batch])
        for start in range(0, len(application_ids), max_items_per_batch)
    ]


def batch_operation_name(batch: Sequence[str], index: int) -> str:
    """Name a map iteration after the applications it holds, so history is searchable."""
    return f'batch-{index:04d}-{batch[0]}'


def missing_feature_fields(row: dict) -> tuple[str, ...]:
    """Required fields the feature store left absent or null."""
    return tuple(field for field in REQUIRED_FEATURE_FIELDS if row.get(field) is None)


def partition_scorable(rows: Sequence[dict]) -> tuple[list[dict], tuple[str, ...]]:
    """Split a batch into rows the model can score and the ids it cannot.

    An incomplete row is dropped and reported rather than failing its batch. One
    application missing a credit score must not cost the other twenty-four theirs.
    """
    scorable: list[dict] = []
    rejected: list[str] = []
    for row in rows:
        if missing_feature_fields(row):
            rejected.append(row.get('applicationId') or UNIDENTIFIED_APPLICATION)
        else:
            scorable.append(row)
    return scorable, tuple(rejected)


def risk_band(probability_of_default: float) -> str:
    if probability_of_default >= DECLINE_THRESHOLD:
        return 'decline'
    if probability_of_default >= REFER_THRESHOLD:
        return 'refer'
    return 'approve'


def score_rows(rows: Sequence[dict], probabilities: Sequence[float]) -> list[dict]:
    """Pair each scored row with its probability and the band it falls in."""
    if len(rows) != len(probabilities):
        raise ValueError(f'endpoint returned {len(probabilities)} probabilities for {len(rows)} rows')
    return [
        {
            'applicationId': row['applicationId'],
            'probabilityOfDefault': probability,
            'band': risk_band(probability),
        }
        for row, probability in zip(rows, probabilities, strict=True)
    ]


def tally_bands(scored: Sequence[dict]) -> dict[str, int]:
    counts: dict[str, int] = dict.fromkeys(RISK_BANDS, 0)
    for row in scored:
        counts[row['band']] += 1
    return counts


def batch_summary(
    batch_index: int,
    scored: Sequence[dict],
    rejected: Sequence[str],
    scores_key: str,
) -> dict:
    """The per-iteration result the map checkpoints: counts and a key, never score rows.

    A map's BatchResult is checkpointed whole, against a 256KB limit, so an
    iteration returns a pointer to its output rather than the output.
    """
    return {
        'batchIndex': batch_index,
        'scoresKey': scores_key,
        'scored': len(scored),
        'rejected': list(rejected),
        'bands': tally_bands(scored),
    }


def roll_up_batches(summaries: Sequence[dict]) -> dict:
    """Fold the iterations that succeeded into the run-level summary."""
    bands: dict[str, int] = dict.fromkeys(RISK_BANDS, 0)
    rejected: list[str] = []
    for summary in summaries:
        for band, count in summary['bands'].items():
            bands[band] += count
        rejected.extend(summary['rejected'])
    return {
        'batches': len(summaries),
        'scored': sum(summary['scored'] for summary in summaries),
        'rejected': sorted(rejected),
        'bands': bands,
    }
