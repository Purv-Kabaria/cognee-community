"""Sync Tally form submissions into cognee memory, then ask about them.

Every run is a full snapshot: new and edited submissions are picked up, and
submissions deleted in Tally (or forms moved to the trash) are forgotten.

Setup (see the README for details):
  1. In Tally: Settings > API keys > Create API key.
  2. Set:
       TALLY_API_KEY=tly-...
       TALLY_FORM_IDS=wMdbjK,3xZ8pQ      (optional, comma-separated; default: every form)
     plus the usual cognee LLM_API_KEY.

Run:
    python examples/example.py ["optional question about your submissions"]
"""

import asyncio
import os
import sys

import cognee

from cognee_community_connector_tally import list_forms, tally_source

DATASET = "tally_demo"


async def main():
    print("=== Forms this API key can see ===")
    for form in list_forms():
        print(f"{form['id']}  {form['name']}  ({form['submissions']} submissions)")

    form_ids = [f for f in os.getenv("TALLY_FORM_IDS", "").split(",") if f.strip()] or None
    source = tally_source(form_ids)

    print("=== Sync ===")
    await cognee.remember(
        source,
        dataset_name=DATASET,
        # The default, spelled out: each run replaces the previous snapshot,
        # which is what makes deleted submissions get forgotten.
        write_disposition="replace",
    )
    # Counts only, never form names or answers.
    print("Sync stats:", source.cognee_sync_stats)

    question = sys.argv[1] if len(sys.argv) > 1 else "What do people ask for most in the forms?"
    answer = await cognee.recall(question, datasets=[DATASET])
    print("Recall:", answer)


if __name__ == "__main__":
    asyncio.run(main())
