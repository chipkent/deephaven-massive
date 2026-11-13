# 79. The user chooses real-time or delayed data, per subscription

Date: 2026-10-31

Status: accepted

`MassiveConfig.delayed_subscriptions` lists the subscriptions that stream from the delayed feed;
every other one uses the real-time feed and fails loudly when the plan cannot serve it. The library
never moves a stream between feeds, and shows nothing as real-time without proof.
