import shutil
import tempfile
from pathlib import Path

from connectors.file_connector import FileConnector

tmp_dir = tempfile.mkdtemp()
tmp_path = Path(tmp_dir)

test_file = tmp_path/"test.txt"
test_file.write_text("Tamil EDA platform test", encoding="utf-8")

connector = FileConnector(name="test",timeout_s=30, base_path=tmp_path)

# test connect
connector.connect()
assert connector.is_connected() is True

# test read
content = connector.read(str(test_file))
assert content == "Tamil EDA platform test"

# test write
connector.write(tmp_path/"test.txt","New written data")
assert (tmp_path / "test.txt").read_text(encoding="utf-8") == "New written data"

#test list
files = connector.list_files(str(tmp_path), pattern="*.txt")
assert len(files) > 0


shutil.rmtree(tmp_dir)
print("All assertions passed.")
