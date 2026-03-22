import importlib
import sys

import pandas as pd


def _import_rendering_utils():
    module_name = "analyst_toolkit.m00_utils.rendering_utils"
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


def test_rendering_utils_imports_without_ipython(monkeypatch):
    monkeypatch.setitem(sys.modules, "IPython", None)
    monkeypatch.setitem(sys.modules, "IPython.display", None)

    module = _import_rendering_utils()
    assert module.to_html_table(pd.DataFrame({"value": [1]}))  # smoke path


def test_to_html_table_escapes_html_by_default():
    module = _import_rendering_utils()
    df = pd.DataFrame(
        {
            "value": ["<script>alert(1)</script>"],
            "label": ["<b>bold</b>"],
        }
    )

    html = module.to_html_table(df)

    assert "<script>" not in html
    assert "<b>bold</b>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert "&lt;b&gt;bold&lt;/b&gt;" in html


def test_to_html_table_can_opt_out_of_escaping():
    module = _import_rendering_utils()
    df = pd.DataFrame({"value": ["<strong>raw</strong>"]})

    html = module.to_html_table(df, escape=False)

    assert "<strong>raw</strong>" in html


def test_markdown_helpers_fall_back_without_ipython(monkeypatch, capsys):
    monkeypatch.setitem(sys.modules, "IPython", None)
    monkeypatch.setitem(sys.modules, "IPython.display", None)

    module = _import_rendering_utils()
    module.display_markdown_summary("Summary", pd.DataFrame({"col": [1, 2]}), max_rows=1)
    module.display_warnings(["first", "second"], title="Warnings")

    out = capsys.readouterr().out
    assert "### Summary" in out
    assert "### Warnings" in out
    assert "- first" in out
    assert "- second" in out
