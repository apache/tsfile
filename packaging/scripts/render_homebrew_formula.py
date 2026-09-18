# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Render the development Formula using an immutable source identity."""

import argparse
import re
from pathlib import Path


VALUE_PATTERNS = {
    "SOURCE_REPOSITORY": r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+",
    "GIT_SHA": r"[0-9a-f]{40}",
    "HOMEBREW_VERSION": r"[0-9][A-Za-z0-9.]*",
    "SOURCE_SHA256": r"[0-9a-f]{64}",
}
TOKEN = re.compile(r"@([A-Z0-9_]+)@")


def render_formula(
    template: str, values: dict[str, str], bottle_block: str = ""
) -> str:
    """Replace declared tokens without interpreting Ruby braces or at-signs."""
    if values.keys() != VALUE_PATTERNS.keys():
        raise ValueError(
            "Formula values must contain exactly the declared source fields"
        )
    for name, pattern in VALUE_PATTERNS.items():
        if not re.fullmatch(pattern, values[name]):
            raise ValueError(f"Invalid Formula value: {name}")
    replacements = {**values, "BOTTLE_BLOCK": bottle_block}
    unknown = set(TOKEN.findall(template)) - replacements.keys()
    if unknown:
        raise ValueError(f"Unknown Formula placeholders: {sorted(unknown)}")
    if not bottle_block:
        # Omit an empty standalone block and its following separator line.
        template = re.sub(r"(?m)^[ \t]*@BOTTLE_BLOCK@\n(?:[ \t]*\n)?", "", template)
    rendered = TOKEN.sub(lambda match: replacements[match.group(1)], template)
    if TOKEN.search(rendered):
        raise ValueError("Unresolved Formula placeholder")
    if len(re.findall(r"^\s*bottle\s+do\b", rendered, re.MULTILINE)) > 1:
        raise ValueError("Duplicate bottle blocks")
    return rendered


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--bottle-block", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    values = {
        "SOURCE_REPOSITORY": args.repository,
        "GIT_SHA": args.git_sha,
        "HOMEBREW_VERSION": args.version,
        "SOURCE_SHA256": args.source_sha256,
    }
    bottle_block = (
        args.bottle_block.read_text(encoding="utf-8") if args.bottle_block else ""
    )
    args.output.write_text(
        render_formula(args.template.read_text(encoding="utf-8"), values, bottle_block),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
