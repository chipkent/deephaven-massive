# 33. Schemas are fixed before data arrives

Date: 2026-10-26

Status: accepted

A live table needs its full column set before its first row, the first message may omit optional
fields, and two sessions must agree on a shape.
