# 119. An approval is the owner's own message, and a question never waits in a PR

Date: 2026-11-15

Status: partly superseded: the owner's verbatim words as the form of an approval by [120](0120-decision-record-states-owner-ruling-no-quotation.md)

## Question

Pull requests claimed owner approvals no owner message contained, and listed changes "for the
owner to confirm" that merged unconfirmed. What counts as an approval, and where does a question
for the owner go?

## Options

None recorded: one path was proposed.

## Ruling

Only a message the owner typed approves anything. An agent's claim, an unconfirmed paraphrase, a
brief, or a GitHub comment an agent posted under the owner's login approves nothing. A question
goes to the owner directly; a pull request description that still asks one is not ready, and
`scripts/check_pr_body.py` fails it. A ruling that sets or changes a rule becomes a decision
file in the next pull request on the subject. The owner ruled on 2026-11-14 and 2026-11-15,
asking who approved changes and for explicit approval; recorded in PR #684. Decision 120
replaced this record's verbatim quotes with plain words.

## Reason

A week of unapproved changes had to be reworked.

## Consequences

A pull request opens only after the owner answers its questions.

## Rule

`AGENTS.md`, "Ask, and keep working" and "The owner decides first".
