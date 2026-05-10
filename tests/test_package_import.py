import oznak


def test_package_import_exposes_version() -> None:
    assert isinstance(oznak.__version__, str)
    assert oznak.__version__
