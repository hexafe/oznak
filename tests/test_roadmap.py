import re
from datetime import date
from pathlib import Path

ROADMAP_PATH = Path("docs/IMPLEMENTATION_ROADMAP.md")
CI_WORKFLOW_PATH = Path(".github/workflows/ci.yml")

REQUIRED_SECTION_HEADINGS = [
    "## Repository review summary",
    "## Phase plan",
    "## Feature-by-feature execution policy",
    "## Tracking cadence",
]

ALLOWED_PHASE_STATUSES = {
    "✅ Completed",
    "🟡 Planned",
    "🟢 In Progress",
    "🚧 In Progress",
    "⛔ Blocked",
    "🔴 Blocked",
}


def _roadmap_content() -> str:
    return ROADMAP_PATH.read_text(encoding="utf-8")


def _extract_phase_rows(content: str) -> list[list[str]]:
    """Return parsed rows from the Phase plan markdown table."""
    phase_section_match = re.search(
        r"## Phase plan\n\n(?P<table>(?:\|.*\|\n)+)",
        content,
    )
    assert phase_section_match, "Roadmap must include a markdown table under '## Phase plan'."

    table_lines = [
        line.strip() for line in phase_section_match.group("table").splitlines() if line.strip()
    ]
    assert table_lines, "Phase plan table must not be empty."

    header = table_lines[0]
    assert header == "| Phase | Scope | Status | Next step |"

    rows: list[list[str]] = []
    for line in table_lines[2:]:  # skip header + divider row
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) != 4:
            continue
        rows.append(cells)

    return rows


def test_implementation_roadmap_exists():
    assert ROADMAP_PATH.exists(), "Roadmap file must exist in docs/."


def test_implementation_roadmap_has_required_sections_and_last_updated_date():
    content = _roadmap_content()

    for heading in REQUIRED_SECTION_HEADINGS:
        assert heading in content, f"Missing required roadmap section: {heading}"

    last_updated_match = re.search(
        r"_Last updated:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})(?:[^\n]*)_",
        content,
    )
    assert last_updated_match, "Roadmap must include a parseable '_Last updated: YYYY-MM-DD_' line."
    date.fromisoformat(last_updated_match.group(1))


def test_implementation_roadmap_phase_table_is_well_formed():
    content = _roadmap_content()
    rows = _extract_phase_rows(content)

    required_phases = [f"Phase {phase_number}" for phase_number in range(6)]
    by_phase = {row[0]: row for row in rows}
    for phase in required_phases:
        assert phase in by_phase, f"Missing {phase} row in roadmap phase table"

    for phase, scope, status, next_step in rows:
        assert scope, f"{phase} must have non-empty Scope"
        assert status, f"{phase} must have non-empty Status"
        assert next_step, f"{phase} must have non-empty Next step"
        assert status in ALLOWED_PHASE_STATUSES, (
            f"{phase} has unsupported status '{status}'. "
            f"Allowed: {sorted(ALLOWED_PHASE_STATUSES)}"
        )


def test_implementation_roadmap_has_feature_policy_language():
    content = _roadmap_content()
    assert "For each new feature merged into the repository" in content


def test_roadmap_ci_claims_align_with_ci_workflow_presence():
    content = _roadmap_content()
    has_ci_workflow = CI_WORKFLOW_PATH.exists()

    ci_presence_claim_patterns = [
        r"CI pipeline is present",
        r"running tests in `\.github/workflows/ci\.yml`",
        r"Wire tests into CI",
    ]
    claims_ci_present = any(re.search(pattern, content) for pattern in ci_presence_claim_patterns)

    if claims_ci_present:
        assert has_ci_workflow, "Roadmap claims CI is present, but .github/workflows/ci.yml is missing."
    else:
        assert not has_ci_workflow, (
            "CI workflow exists but roadmap does not claim CI presence; "
            "update roadmap text to avoid drift."
        )
