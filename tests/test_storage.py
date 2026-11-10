import json
import pytest
from raftlite.core import LogEntry
from raftlite.storage import Storage, StorageError


def mk(n, term=1, start=1):
    return [LogEntry(term, start + i, f"k{start+i}", f"v{start+i}") for i in range(n)]


def test_diretorio_vazio_da_estado_inicial(tmp_path):
    s = Storage(tmp_path / "n1")
    assert s.load_meta() == (0, None)
    assert s.load_log() == []
    assert s.load_db() == ({}, 0)


def test_meta_sobrevive_a_reinicio(tmp_path):
    Storage(tmp_path).save_meta(7, 3)
    assert Storage(tmp_path).load_meta() == (7, 3)  # nova instancia = "reinicio"


def test_meta_voto_nulo(tmp_path):
    Storage(tmp_path).save_meta(2, None)
    assert Storage(tmp_path).load_meta() == (2, None)


def test_log_append_e_reload_preservam_ordem(tmp_path):
    s = Storage(tmp_path)
    s.append_entries(mk(3))
    s.append_entries(mk(2, term=2, start=4))
    got = Storage(tmp_path).load_log()
    assert [e.index for e in got] == [1, 2, 3, 4, 5]
    assert [e.term for e in got] == [1, 1, 1, 2, 2]
    assert got[0].key == "k1" and got[4].value == "v5"


def test_truncate_log_from(tmp_path):
    s = Storage(tmp_path)
    s.append_entries(mk(5))
    s.truncate_log_from(3)
    assert [e.index for e in Storage(tmp_path).load_log()] == [1, 2]


def test_truncate_alem_do_fim_nao_muda_nada(tmp_path):
    s = Storage(tmp_path)
    s.append_entries(mk(2))
    s.truncate_log_from(10)
    assert len(s.load_log()) == 2


def test_append_depois_de_truncar(tmp_path):
    s = Storage(tmp_path)
    s.append_entries(mk(5))
    s.truncate_log_from(3)
    s.append_entries(mk(1, term=2, start=3))
    got = s.load_log()
    assert [(e.index, e.term) for e in got] == [(1, 1), (2, 1), (3, 2)]


def test_db_roundtrip(tmp_path):
    s = Storage(tmp_path)
    s.save_db({"x": "10", "y": "ç"}, 4)
    assert Storage(tmp_path).load_db() == ({"x": "10", "y": "ç"}, 4)


def test_escrita_atomica_nao_deixa_tmp(tmp_path):
    s = Storage(tmp_path)
    s.save_meta(1, 1)
    s.save_db({}, 0)
    assert not list(tmp_path.glob("*.tmp"))


def test_ultima_linha_truncada_por_crash_e_descartada(tmp_path):
    s = Storage(tmp_path)
    s.append_entries(mk(2))
    with open(s.log_path, "a") as f:
        f.write('{"term": 1, "index": 3, "ke')  # escrita interrompida
    got = Storage(tmp_path).load_log()
    assert [e.index for e in got] == [1, 2]
    # arquivo foi reparado: novo append continua valido
    s.append_entries(mk(1, start=3))
    assert [e.index for e in s.load_log()] == [1, 2, 3]


def test_corrupcao_no_meio_do_log_e_erro(tmp_path):
    s = Storage(tmp_path)
    s.append_entries(mk(3))
    lines = s.log_path.read_text().split("\n")
    lines[1] = "lixo"
    s.log_path.write_text("\n".join(lines))
    with pytest.raises(StorageError):
        s.load_log()
