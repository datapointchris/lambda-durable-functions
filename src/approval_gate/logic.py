"""Pure decisions. No SDK types, no clients, no context.

Everything here is callable from a plain pytest test with plain dicts and strings.
"""

import json
from dataclasses import dataclass

APPROVED = 'approved'
DECLINED = 'declined'

REVIEWER_REJECTED = 'reviewer-rejected'
WINDOW_LAPSED = 'window-lapsed'
REVIEW_SERVICE_SILENT = 'review-service-silent'
UNSIGNED_DECISION = 'unsigned-decision'

WINDOW_TIMEOUT_MARKER = 'Callback.Timeout'
HEARTBEAT_TIMEOUT_MARKER = 'Callback.Heartbeat'

REQUIRED_REQUEST_FIELDS = ('changeRequestId', 'targetTable', 'statement', 'requestedBy')


@dataclass(frozen=True)
class ChangeRequest:
    """One proposed change to a production table."""

    request_id: str
    target_table: str
    statement: str
    requested_by: str


@dataclass(frozen=True)
class Decision:
    """What the gate concluded, however it concluded it."""

    verdict: str
    reviewer: str | None = None
    reason: str | None = None
    note: str = ''


def parse_change_request(event: dict) -> ChangeRequest:
    """Read the request off the event, refusing one that cannot be reviewed."""
    missing = [field for field in REQUIRED_REQUEST_FIELDS if not event.get(field)]
    if missing:
        raise ValueError(f'change request is missing {", ".join(missing)}')
    return ChangeRequest(
        request_id=event['changeRequestId'],
        target_table=event['targetTable'],
        statement=event['statement'],
        requested_by=event['requestedBy'],
    )


def build_review_message(request: ChangeRequest, callback_token: str) -> dict:
    """The body the approval service dequeues. The token is what it answers with."""
    return {
        'changeRequestId': request.request_id,
        'targetTable': request.target_table,
        'statement': request.statement,
        'requestedBy': request.requested_by,
        'callbackToken': callback_token,
    }


def parse_decision(answer: str | None) -> Decision:
    """Read the approval service's answer.

    An answer nobody signed is not an approval, however it is spelled — the audit
    row has to name a reviewer, so a payload without one can only be declined.
    """
    try:
        parsed = json.loads(answer) if answer else None
    except json.JSONDecodeError:
        parsed = None
    if not isinstance(parsed, dict) or not parsed.get('reviewer'):
        return Decision(verdict=DECLINED, reason=UNSIGNED_DECISION)

    reviewer = parsed['reviewer']
    note = parsed.get('note', '')
    if parsed.get('verdict') != APPROVED:
        return Decision(verdict=DECLINED, reviewer=reviewer, reason=REVIEWER_REJECTED, note=note)
    return Decision(verdict=APPROVED, reviewer=reviewer, note=note)


def classify_gate_failure(message: str | None) -> str:
    """Why the gate failed.

    A rejection, a lapsed window and a dead approval service all reach the handler
    as one exception type, so the message is the only thing separating them.
    """
    text = message or ''
    if HEARTBEAT_TIMEOUT_MARKER in text:
        return REVIEW_SERVICE_SILENT
    if WINDOW_TIMEOUT_MARKER in text:
        return WINDOW_LAPSED
    return REVIEWER_REJECTED


def review_outcome_item(request: ChangeRequest, decision: Decision, decided_epoch: float) -> dict:
    """The audit row. A declined change is recorded as fully as an applied one."""
    item = {
        'changeRequestId': {'S': request.request_id},
        'targetTable': {'S': request.target_table},
        'requestedBy': {'S': request.requested_by},
        'verdict': {'S': decision.verdict},
        'decidedAt': {'N': f'{decided_epoch:.0f}'},
    }
    if decision.reviewer:
        item['reviewer'] = {'S': decision.reviewer}
    if decision.reason:
        item['reason'] = {'S': decision.reason}
    if decision.note:
        item['note'] = {'S': decision.note}
    return item
