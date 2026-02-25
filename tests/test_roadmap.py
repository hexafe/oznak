from pathlib import Path


def test_implementation_roadmap_exists():
    roadmap = Path("docs/IMPLEMENTATION_ROADMAP.md")
    assert roadmap.exists(), "Roadmap file must exist in docs/."


def test_implementation_roadmap_has_phase_table_and_policy():
    content = Path("docs/IMPLEMENTATION_ROADMAP.md").read_text(encoding="utf-8")

    assert "| Phase | Scope | Status | Next step |" in content

    required_phases = [
        "Phase 0",
        "Phase 1",
        "Phase 2",
        "Phase 3",
        "Phase 4",
        "Phase 5",
    ]
    for phase in required_phases:
        assert phase in content, f"Missing {phase} in roadmap"

    assert "For each new feature merged into the repository" in content
