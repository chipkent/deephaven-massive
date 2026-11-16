# 120. A decision record states the owner's ruling, not a quotation

Date: 2026-11-15

Status: accepted

## Question

Should decision records state the owner's intent instead of quoting the owner, and how does a
pull request show an approval?

## Options

1. Keep decision 119: every approval and every record holds the owner's words, verbatim and
   dated.
2. A record states the ruling in plain words, and a pull request cites the record or, before
   one exists, shows the question with the owner's dated answer.

## Ruling

Option 2, decided by the owner in the coordinating chat session on 2026-11-15, agreeing with
the recommendation put to them. A decision file states the ruling in plain words, with no
quotation: the question asked, the ruling, its reason, its date, and where it was decided.
Before writing the file, the agent reads its wording back to the owner, a subagent through the
agent that started it, and the pull request shows the file's text. A pull request that claims
an owner approval cites the decision file by number. Before a file exists, it shows the
question together with the owner's dated answer; a question alone approves nothing.
`scripts/check_pr_body.py` stays, with its wording updated.

## Reason

A quotation read later without its question is stretched or misread, while a statement of
intent says what the ruling covers.

## Consequences

The check confirms only the shape; whether the answer covers the change is the owner's to
judge. This replaces the part of decision 119 that made the owner's verbatim words the form of
an approval; the rest of 119 holds.

## Rule

`AGENTS.md`, "An approval is a decision file or the owner's answer to a question", and the
Workflow bullet on decision files.
