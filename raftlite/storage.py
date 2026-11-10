"""Persistencia em disco: meta.json, log.jsonl, db.json.

Escritas de arquivos inteiros sao atomicas (tmp + fsync + rename). O log
usa append com fsync; um ultimo registro truncado por crash e descartado
no carregamento.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .core import LogEntry


class StorageError(Exception):
    pass


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write(path: Path, data: str) -> None:
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _entry_to_line(e: LogEntry) -> str:
    return json.dumps(
        {"term": e.term, "index": e.index, "key": e.key, "value": e.value},
        ensure_ascii=False,
    )


class Storage:
    def __init__(self, directory):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.meta_path = self.dir / "meta.json"
        self.log_path = self.dir / "log.jsonl"
        self.db_path = self.dir / "db.json"

    # ---- meta (termo + voto) ----
    def save_meta(self, term: int, voted_for: Optional[int]) -> None:
        atomic_write(self.meta_path,
                     json.dumps({"current_term": term, "voted_for": voted_for}))

    def load_meta(self) -> Tuple[int, Optional[int]]:
        if not self.meta_path.exists():
            return 0, None
        d = json.loads(self.meta_path.read_text(encoding="utf-8"))
        return int(d["current_term"]), d["voted_for"]

    # ---- log ----
    def append_entries(self, entries: Iterable[LogEntry]) -> None:
        entries = list(entries)
        if not entries:
            return
        with open(self.log_path, "a", encoding="utf-8") as f:
            for e in entries:
                f.write(_entry_to_line(e) + "\n")
            f.flush()
            os.fsync(f.fileno())
        _fsync_dir(self.dir)

    def load_log(self) -> List[LogEntry]:
        if not self.log_path.exists():
            return []
        lines = self.log_path.read_text(encoding="utf-8").split("\n")
        entries: List[LogEntry] = []
        torn = False
        for pos, raw in enumerate(lines):
            if not raw.strip():
                continue
            try:
                d = json.loads(raw)
                entries.append(LogEntry(int(d["term"]), int(d["index"]),
                                        d["key"], d["value"]))
            except (ValueError, KeyError, TypeError):
                rest = [l for l in lines[pos + 1:] if l.strip()]
                if rest:
                    raise StorageError(f"log corrompido na linha {pos + 1}")
                torn = True  # ultima linha incompleta (crash no meio da escrita)
        if torn:
            self._rewrite_log(entries)
        return entries

    def truncate_log_from(self, index: int) -> None:
        kept = [e for e in self.load_log() if e.index < index]
        self._rewrite_log(kept)

    def _rewrite_log(self, entries: List[LogEntry]) -> None:
        atomic_write(self.log_path, "".join(_entry_to_line(e) + "\n" for e in entries))

    # ---- banco final (somente comitados) ----
    def save_db(self, db: Dict[str, str], last_applied: int) -> None:
        atomic_write(self.db_path,
                     json.dumps({"last_applied": last_applied, "data": db},
                                ensure_ascii=False))

    def load_db(self) -> Tuple[Dict[str, str], int]:
        if not self.db_path.exists():
            return {}, 0
        d = json.loads(self.db_path.read_text(encoding="utf-8"))
        return dict(d["data"]), int(d["last_applied"])
