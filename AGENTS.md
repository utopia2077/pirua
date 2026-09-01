## Commits

Commit at the boundary of a complete unit of work — one feature, fix, or refactor, including its tests. Each commit leaves the repository in a valid state and is independently reviewable and revertible. Group intermediate steps into one commit; merge a commit that only makes sense alongside its neighbor, and split one that bundles independent changes. Fix-ups amend or squash the original commit. Inspect the staged diff before committing; include only changes belonging to the commit.

Use Conventional Commits: `type(scope): summary`. Keep the summary imperative, outcome-focused, and no longer than 72 characters; explain non-obvious reasons in the body.
