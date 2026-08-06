# This module has been removed.
# Range/chunk planning was removed during refactoring — it added complexity
# with no measurable throughput benefit. Prefix mode downloads full objects
# via get_object(); single-object mode downloads directly without range splitting.
