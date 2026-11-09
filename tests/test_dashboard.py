"""Smoke test: the dashboard renders against a freshly built warehouse without exceptions."""

from datetime import date

import pytest

from lakehouse import pipeline

AppTest = pytest.importorskip("streamlit.testing.v1").AppTest


def test_dashboard_renders(settings, monkeypatch):
    pipeline.run(settings, start=date(2024, 1, 1), end=date(2024, 4, 30))
    monkeypatch.setenv("LAKEHOUSE_WAREHOUSE", str(settings.warehouse_path))

    at = AppTest.from_file("../dashboard/app.py", default_timeout=120).run()
    assert not at.exception
    assert at.metric[0].value == "20"

    at.sidebar.multiselect[0].set_value(["Financials"]).run()
    assert not at.exception
    assert at.metric[0].value == "6"
