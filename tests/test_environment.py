def test_required_packages_are_importable():
    import opensearchpy  # noqa: F401
    import psycopg2  # noqa: F401
    import pyflink  # noqa: F401


def test_python_version():
    import sys

    assert sys.version_info[:2] == (3, 11)

