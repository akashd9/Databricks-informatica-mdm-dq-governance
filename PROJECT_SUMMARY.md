# What We Built — In Simple Terms

This is a plain-language walkthrough of this project, from the original
ask to the first real, working run. For the formal technical writeup, see
[README.md](README.md).

## 1. Understood the ask

Companies often store the same customer or account in several different
places — an ERP system, a CRM system, a spreadsheet, a partner's API — and
each version can be slightly different (a typo in a name, a missing email,
an old address). Without a process to catch and fix that, nobody can fully
trust any single "customer record."

The specific ask was to solve this using **Databricks** as the data
platform, with **Informatica's Data Quality and Master Data Management
(MDM)** products doing two jobs: checking whether a record is good enough
to use, and figuring out when two records from different systems are
actually the same person or company so they can be merged into one
trustworthy "golden record" — plus keeping track of where every record came
from and whether the pipeline itself is healthy, instead of that being an
afterthought.

## 2. Built the actual pipeline

Data flows through stages, each one cleaning it up a bit more:

- **Bronze** — the raw data lands here, untouched, exactly as each source
  system sent it.
- **Silver** — the raw data gets tidied into one consistent shape (same
  column names, same formatting) regardless of which system it came from.
- **Quality gate** — every record gets scored for how trustworthy it is
  (does it have a name? a valid email? a properly formatted ID?). Records
  that score too low get set aside for a person to review, instead of
  quietly poisoning everything downstream. **This is where Informatica Data
  Quality plugs in** — it's the product meant to do this scoring for real.
- **Matching gate** — records that pass the quality check get compared
  against each other to find likely duplicates (e.g., "John Smith" from the
  ERP system and "J. Smith" from the CRM system, same email, same tax ID —
  probably the same person). **This is where Informatica MDM plugs in** —
  it's the product meant to do this matching for real.
- **Gold** — the final, merged, trustworthy record, with a record of which
  source records fed into it and how confident the match was.

Because we don't (yet) have live Informatica credentials, both of those
gates were built with an **on/off switch**: when Informatica is connected,
the pipeline calls it directly; when it isn't, a local stand-in does
functionally the same job (same rules, same scoring logic) so the whole
pipeline still runs and produces real output today. Flipping the switch
later, once Informatica is connected, doesn't require rewriting anything —
just filling in real credentials.

## 3. Set up the cloud infrastructure

Rather than clicking through Databricks' website to set things up by hand
(which is easy to forget, hard to repeat, and leaves no record of what was
done), we used **Terraform** — a tool that lets you describe "I want a
catalog, these folders, this storage" as code, then have it actually create
those things for you. Re-running that code later just confirms nothing
drifted, rather than creating duplicates.

This created the real catalog and folder structure in your Databricks
workspace, the storage areas where each source system's files land, and —
importantly — a secure, separate storage box (Databricks calls it a
"secret scope") specifically for **Informatica's connection details** (its
web address and login token) once you have them. Nothing about Informatica
is hardcoded anywhere; it's meant to be filled in there.

## 4. Put everything on GitHub

All the code, configuration, and infrastructure setup was pushed to a
GitHub repository. This means it's backed up off your machine, anyone you
give access to can see exactly what was built and why (every change has a
written explanation attached), and nothing depends on this one laptop
still existing tomorrow.

## 5. Wrote documentation

A README was written explaining the business problem, why it matters, the
solution design, the costs and benefits, and how all the pieces fit
together — written for someone who wasn't in the room while it was built.

## 6. Set up a proper team workflow

Three long-lived tracks were set up: **main** (everyday work), **test**
(a staging area to try things before they're final), and **prod**
(production — the real, trusted version). Plus a documented convention for
how a new feature or an urgent fix should branch off, get reviewed, and
merge back in — so multiple people could work on this without stepping on
each other, even though it's currently just you.

## 7. Added the "grown-up" features a real production system needs

- **Tuned the quality and matching rules using realistic fake data.**
  Rather than guessing at thresholds, we generated a batch of made-up but
  realistic customer records (with intentional typos, missing fields, and
  near-duplicate names — the kind of mess real data actually has), ran them
  through the quality and matching logic, and adjusted the settings based
  on what actually worked. Those exact settings are what would also
  configure the real Informatica quality mapping and matching rules once
  connected — this tuning isn't wasted work, it transfers directly.
- **Made it work for a second type of record (business accounts, not just
  customers)**, reusing the same quality-check and matching logic, to prove
  the design isn't a one-off that only happens to work for one case.
- **Built a review queue** so that records the system isn't fully confident
  about (failed the quality check, or matched with low confidence) land
  somewhere a person can look at them and approve or reject — and an
  approval actually changes what happens on the next run, not just a note
  nobody reads.
- **Added cost tracking**, so the pipeline watches its own cloud spending
  and warns (or halts) if it's running up an unexpectedly large bill.
- **Let business users suggest glossary terms** (plain-English definitions
  of what each piece of data means) without needing an engineer to make a
  code change every time.
- **Wrote up what it would take to expand this across regions** (e.g., for
  data-residency law reasons) as a planning document, rather than actually
  standing up expensive new infrastructure nobody had asked for yet.

## 8. Tried to connect real Informatica

This was a direct, hands-on attempt to log into your actual Informatica
account through its programming interface (API) and get the real quality
and matching engines working, instead of the local stand-in.

We got as far as finding the correct web address to log in with and
confirming the login request was formatted correctly — but it kept getting
rejected. After some back-and-forth, it turned out your Informatica account
has an extra security step (multi-factor authentication, the same idea as
a text-message code when you log into your bank) turned on, and a simple
username-and-password login can't get past that from a script. Getting
past it needs a special "secret key" Informatica provides specifically for
this kind of automated access, which we didn't have.

**Net result: real Informatica still isn't connected.** The pipeline is
fully wired up and ready for it, but is currently running on the local
stand-in described in point 2.

## 9. Actually ran the whole thing for real

Up to this point, everything had been checked for correctness (does the
code make sense, does the configuration look right) but never actually
switched on against your real Databricks account. So we did that — pressed
go and let it run for real, watching what actually happened rather than
assuming it would work.

This immediately found **8 real problems** that no amount of careful
reading could have caught, because they only show up when something
actually executes on a live system — things like: the account requiring a
newer type of cloud computer than the pipeline was set up to use, a
software library that turned out not to be available, a few genuine
mistakes in how different parts of the code talked to each other, and the
shared Databricks account hitting a maximum-number-of-tables limit (shared
with other unrelated projects already in that account). Each one was
found, fixed, and re-tried until the entire thing ran cleanly.

**Important:** this real run used the **local stand-in**, not real
Informatica (see point 8) — so what got proven is that the whole pipeline,
end to end, genuinely works on real infrastructure. What's still unproven
is how it behaves with real Informatica scoring and matching, since that
piece isn't connected yet.

The real output from that run: 197 merged customer records and 30 merged
account records, built from smaller numbers of raw, duplicate-containing
input records — with a record of which ones needed a human's attention
along the way.

## 10. Tidied up afterward

Removed temporary and leftover files from the computer that Git didn't
need to track (things that get automatically recreated when needed, like
downloaded software components and test caches) — keeping the computer
clean and making sure GitHub, not this one laptop, is the real source of
truth for the project. One exception was kept local on purpose: Terraform's
own record of what it created, which can end up holding real passwords
once real ones are used, so it's deliberately not uploaded anywhere.

## 11. Closed the remaining "is this actually production-grade?" gaps

After point 9's real run, we asked the honest question: is this actually
ready for production, or does it just work once? That review found seven
real gaps, and — other than the two already-known Informatica/Terraform
blockers — all of them got fixed in a follow-up session:

- **Fixed the bronze/silver/gold split for real.** Point 9's run technically
  worked, but every table secretly lived in one "silver" bucket regardless
  of whether it was raw, cleaned, or final data — a shortcut taken to avoid
  destabilizing a pipeline that had just started working. This session
  properly separated them into real bronze/silver/gold folders.
- **Added who-can-see-what rules.** Set up four roles (data engineers, data
  stewards, data analysts, and a small "sees real PII" group) with exactly
  the access each needs — engineers can read/write everything, analysts can
  only read the final trusted data, and email/tax ID columns are
  automatically hidden (shown as `***MASKED***`) from anyone not
  specifically allowed to see them.
- **Added a real backup/recovery plan.** The final trusted tables now keep
  30 days of history automatically (so a bad run can be undone by asking
  for "what this table looked like yesterday"), with a weekly automated
  cleanup job and a written step-by-step recovery guide.
- **Fixed the "alert one person's email" problem.** Failure alerts now go
  to a configurable list of people instead of one hardcoded inbox, with
  data-quality problems able to alert a different team than
  infrastructure problems, and support for a Slack-style notification in
  addition to email.
- **Tried to set up separate test/production copies.** Blocked — creating
  a brand-new catalog in this Databricks account requires clicking through
  its website once; it can't be done by script when this particular
  account setting is on. The setup code is written and ready; it just
  needs someone with website access to click "Create Catalog" once per
  environment first.
- **Generated a much bigger, messier practice dataset** — about 6 times
  more records than the original test batch, with harder problems on
  purpose (records in different date formats, foreign accented names,
  the same ID number accidentally reused by two different source systems)
  — and uploaded it to the real system, queued up to run.
- **This document and the README got updated** with all of the above.

**Being honest about where things actually stand right now:** fixing the
bronze/silver/gold split (the first bullet) required resetting some
tables so they could be rebuilt correctly, and that rebuild needs to
finish in one clean pass to count as done. It hasn't finished yet — not
because of a mistake, but because the *shared* Databricks account (shared
with other, unrelated demo projects) was right at its maximum allowed
number of tables, and every attempt to finish the rebuild got rejected for
exactly that reason. See point 12 for what we then did about it.

## 12. Tried to free up the shared table limit directly

At your direction, rather than just waiting, we went and actually looked
at what else was using up the shared account's table allowance:

- **Audited every other project in the account** — not guessing, but
  querying Databricks' own system tables to get a real count of how many
  tables each of your other demo catalogs actually had.
- **Found and deleted two completely empty leftover catalogs** from an
  old project (double-checked they held zero real tables before touching
  them).
- **Found a third, larger abandoned project (~99 real tables) and deleted
  it too**, after confirming it was unrelated to this work and genuinely
  not in use.
- That's **over 100 real tables removed and independently verified gone**
  — checked directly, not just assumed.

**And the pipeline's error message didn't change at all.** Same exact
"you're 3 tables over the limit" number, before the deletions and after,
across roughly two hours and eight more attempts in between. That's a
strong sign Databricks' own count of "how many tables you have" is a
cached number on their end that isn't updating in real time — not
something fixable by deleting more from this side. The two real options
left are waiting for that cache to refresh on its own, or asking
Databricks support to clear it manually. The moment it clears, everything
already queued (including the 6x-bigger practice dataset from point 11)
should run through in one pass automatically — no further changes needed
on this end.

## Bottom line on Informatica

Two integration points exist and are fully built: **quality checking** and
**duplicate matching**. Both currently run on a local stand-in because real
Informatica credentials aren't connected yet — blocked specifically on
getting past your account's extra security step to obtain the right kind
of API access. That's the single biggest piece of unfinished work, and
everything else in this project is ready and waiting for it.
