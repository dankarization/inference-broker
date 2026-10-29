"""Кликовая сортировка таблиц для локальных HTML-дашбордов.

Подключение: вставьте ``TABLE_SORT_CSS`` в <style> и ``TABLE_SORT_JS``
внутрь существующего <script> (без <script src>, страница остаётся
самодостаточной). Затем либо ``TableSort.bind(table)``, либо
``TableSort.nextState`` / ``TableSort.sortRows`` для таблиц из JS-данных.
"""
from pathlib import Path

TABLE_SORT_JS = (Path(__file__).resolve().parent / "table_sort.js").read_text(encoding="utf-8")  # рядом с dashboard.py

TABLE_SORT_CSS = """
.sort-btn{appearance:none;border:0;background:none;color:inherit;font:inherit;text-transform:inherit;letter-spacing:inherit;padding:2px 0;cursor:pointer;white-space:nowrap}
.sort-btn:hover{filter:brightness(0.85)}
.sort-btn:focus-visible{outline:2px solid currentColor;outline-offset:2px;border-radius:2px}
.sort-mark{display:inline-block;min-width:12px;margin-left:3px;opacity:.85}
"""
