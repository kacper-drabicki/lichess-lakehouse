"""pgn_parsing.py

Modular streaming parser for large-scale PGN files.
"""

from typing import Dict, Generator, Iterable, Iterator, List


def stream_pgn_records(
    lines: Iterable[str], initial_id: int = 1
) -> Generator[Dict[str, object], None, None]:
  """Stream parsed individual PGN games from an iterable of lines.

  Args:
      lines: Iterable yielding file lines as strings.
      initial_id: The starting sequence integer for game_id.

  Yields:
      Dict with keys:
          - 'game_id': int
          - 'pgn': str (untrimmed internal structure, stripped outer whitespace)
  """
  current_game_lines: List[str] = []
  game_id = initial_id

  for line in lines:
    # A new PGN entry starts when [Event is encountered and prior lines exist
    if line.startswith("[Event") and current_game_lines:
      game_content = "".join(current_game_lines).strip()
      if game_content:
        yield {"game_id": game_id, "pgn": game_content}
        game_id += 1
      current_game_lines = []

    current_game_lines.append(line)

  # Flush EOF remnant
  if current_game_lines:
    game_content = "".join(current_game_lines).strip()
    if game_content:
      yield {"game_id": game_id, "pgn": game_content}


def batch_generator(
    records: Iterator[Dict[str, object]], batch_size: int = 10000
) -> Generator[List[Dict[str, object]], None, None]:
  """Chunk an iterator of records into fixed-size batches.

  Args:
      records: Iterator yielding dictionary records.
      batch_size: Maximum elements per yielded batch.

  Yields:
      List of dictionary records of length <= batch_size.
  """
  if batch_size <= 0:
    raise ValueError(
        f"batch_size must be a positive integer, got {batch_size}"
    )

  current_batch: List[Dict[str, object]] = []
  for record in records:
    current_batch.append(record)
    if len(current_batch) >= batch_size:
      yield current_batch
      current_batch = []

  if current_batch:
    yield current_batch