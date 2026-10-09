import pgqueue


def test_version_is_exposed() -> None:
    assert pgqueue.__version__ == "0.1.0"
