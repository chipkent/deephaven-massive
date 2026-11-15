# 114. The code never knows Massive's plans; it learns from Massive's answers

Date: 2026-11-15

Status: accepted

The library and its tests work with whatever Massive plans a user holds, and keep working when the user changes plans or Massive changes its offerings. Massive decides what each key may read, how late, and how fast. The code learns these things in two ways only: from Massive's answers (an entitlement refusal, a rate-limit answer), and from settings the user gives. It never learns them from a plan's name, from a probe for the user's plan, or from a built-in table of what a plan includes, such as a tier's request rate or delay. A live test sends its requests and, when Massive refuses one for entitlement, skips and quotes Massive's refusal.

The reason is that Massive owns its plans and changes them. The owner, on 2026-11-11: "it is problematic for the software to know massive's plan tiers. Massive can change these in time. I've seen data vendors keep old users on a legacy plan while moving new or monthly users to a new plan. I'm concerned that the changes I think you are proposing will be extremely brittle in practice." On 2026-10-29: "The plan needs to work no matter what I buy, and I may scale up what I'm buying at any given point." On 2026-11-15, after the live tests' plan probe stopped a whole run because Massive's weekend forex snapshot came back empty: "We have REPEATEDLY discussed that this is not acceptable."

User documentation may describe Massive's offerings, written so it is easy to update. The owner, on 2026-10-30: "I think you want to show what massive supports with a plan, that is fine. Just make it easy to update."
