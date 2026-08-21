import sys
sys.path.append('..')

import io
import pytest
from src.pgn_parsing import stream_pgn_records, batch_generator

SAMPLE_DATA = """[Event "Game 1"]
[Site "https://lichess.org/1"]
1. e4 e5 1-0

[Event "Game 2"]
[Site "https://lichess.org/2"]
1. d4 d5 0-1"""

# 1. PGN Stream Parsing Tests
def test_parse_pgn_stream_extracts_all_games_and_sequential_ids():
  """Ensures standard multi-game files are split correctly with sequential IDs."""
  lines = io.StringIO(SAMPLE_DATA)
  records = list(stream_pgn_records(lines))

  assert len(records) == 2
  assert records[0]["game_id"] == 1
  assert records[0]["pgn"].startswith('[Event "Game 1"]')
  assert records[1]["game_id"] == 2
  assert records[1]["pgn"].startswith('[Event "Game 2"]')


def test_parse_pgn_stream_handles_empty_input():
  """Ensures empty files do not crash the generator or emit dummy records."""
  assert list(stream_pgn_records(io.StringIO(""))) == []


def test_parse_pgn_stream_flushes_last_game_at_eof():
  """Critical: ensures the final game is not dropped when EOF is reached without a trailing newline."""
  single_game = '[Event "Final Game"]\n1. Nf3 Nf6 1/2-1/2'
  records = list(stream_pgn_records(io.StringIO(single_game)))

  assert len(records) == 1
  assert records[0]["pgn"] == single_game


# 2. Batching Tests
def test_batch_iterable_handles_partial_and_exact_batches():
  """Ensures batching preserves all elements across uneven chunk sizes."""
  items = [1, 2, 3, 4, 5]
  batches = list(batch_generator(items, batch_size=2))

  assert batches == [[1, 2], [3, 4], [5]]


def test_batch_iterable_invalid_size_fails_fast():
  """Guards against infinite loops or invalid configurations."""
  with pytest.raises(ValueError):
    list(batch_generator([1, 2], batch_size=0))