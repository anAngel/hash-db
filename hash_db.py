# Add import
import threading

# Modify the HashEntry class to include a progress callback
class HashEntry:
    def __init__(self, filename, size=None, mtime=None, hash=None, type=None, progress_callback=None):
        self.filename = filename
        self.size = size
        self.mtime = mtime
        self.hash = hash
        self.type = type
        self.progress_callback = progress_callback

    def hash_file(self):
        if self.filename.is_file():
            if lstat(str(self.filename)).st_size > 0:
                with self.filename.open('rb') as f:
                    with mmap(f.fileno(), 0, access=ACCESS_READ) as m:
                        result = HASH_FUNCTION(m).hexdigest()
                        if self.progress_callback:
                            self.progress_callback(self.filename)
                        return result
            else:
                return EMPTY_FILE_HASH
        elif self.filename.is_symlink():
            target = readlink(str(self.filename))
            result = HASH_FUNCTION(fsencode(target)).hexdigest()
            if self.progress_callback:
                self.progress_callback(self.filename)
            return result

    def update(self):
        self.update_attrs()
        self.update_type()
        self.hash = self.hash_file()

    def verify(self):
        result = self.hash_file() == self.hash
        if self.progress_callback:
            self.progress_callback(self.filename)
        return result

# Modify the HashDatabase class to include a progress counter
class HashDatabase:
    def __init__(self, path: Path):
        try:
            self.path = find_hash_db(path).parent
        except FileNotFoundError:
            self.path = path
        self.entries = {}
        self.version = DATABASE_VERSION
        self.lock = threading.Lock()
        self.progress_count = 0
        self.total_files = 0

    def progress_callback(self, filename):
        with self.lock:
            self.progress_count += 1
            print(f'\rProcessed {self.progress_count}/{self.total_files} files: {filename}', end='')

    def update(self):
        added, removed, modified = self._find_changes()
        self.total_files = len(added | modified)
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(entry.update): entry for entry in added | modified}
            for future in as_completed(futures):
                entry = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f"Exception while updating {entry.filename}: {exc}")
        print()  # Newline after progress output
        for entry in added:
            self.entries[entry.filename] = entry
        for entry in removed:
            del self.entries[entry.filename]
        content_modified = set()
        for entry in modified:
            old_hash = entry.hash
            entry.update()
            if entry.hash != old_hash:
                content_modified.add(entry)
        return (
            {entry.filename for entry in added},
            {entry.filename for entry in removed},
            {entry.filename for entry in content_modified},
        )

    def verify(self, verbose_failures=False):
        modified = set()
        removed = set()
        count = len(self.entries)
        self.total_files = count
        i = 0
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(entry.verify): entry for entry in self.entries.values()}
            for future in as_completed(futures):
                entry = futures[future]
                try:
                    if future.result():
                        entry.update_attrs()
                    else:
                        if verbose_failures:
                            stderr.write(f'\r{entry.filename} failed hash verification\n')
                        modified.add(entry.filename)
                except FileNotFoundError:
                    removed.add(entry.filename)
                    if verbose_failures:
                        stderr.write(f'\r{entry.filename} is missing\n')
                i += 1
                stderr.write(f'\rChecked {i} of {count} files')
        if i:
            stderr.write('\n')
        return modified, removed
