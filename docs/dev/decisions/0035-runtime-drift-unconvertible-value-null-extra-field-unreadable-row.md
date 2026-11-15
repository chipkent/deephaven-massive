# 35. Runtime drift serves what is right

Date: 2026-10-30

Status: accepted

An extra field is dropped and reported; a value that does not fit is null with one WARNING per
dataset and field; a row whose key cannot be read is dropped and counted. Only a payload in which no
row can be placed fails, so the table never silently lies.
