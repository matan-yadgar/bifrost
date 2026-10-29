# PR follow-up ownership

The bridge owns PR scans, review fingerprints, CI failure delivery, and the shared PR-to-fork route cache. Combine review and CI items for one PR into the existing dispatch queue; do not introduce coordinator tasks, task-management APIs, or concrete provider logic.

CI state lives in the same versioned state file. Forks may update only their existing PR entry's paused, paused_at, reason, and resume_condition fields. A pause expires after ten minutes. All runtime state transactions use the neighboring .update.lock, separate from the service instance lock. Reread pauses after waiting for a dispatch lane and preserve the latest pause fields before saving. Do not hold the state lock while a harness runs.

Review versions are marked delivered after acknowledged dispatch. CI attempt identities are marked reported only after acknowledged dispatch, and only for checks actually included in the prompt. Suppress reported attempts on the same head; new heads clear reported identities and pauses expire after ten minutes. Prune closed/merged PR state only after a complete successful repository scan.
