import os
import hashlib
import shutil
import threading
import signal
import sys
import json
import queue
from typing import Callable, Iterable, Generator
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

type FileHandler = Callable[[Path,Path], None]

class FileSystemSnapshot:
    def __init__(self,
                 snapshots_dir:    str,
                 snapshot_name:    str,
                 target_paths:     list[str],
                 exclude_patterns: list[str]=[],
                 hash_algorithm:   str='blake2b',
                 threads:          int=4,
                 dry_mode:         bool=True,
                 verbose:          bool=False):

        self.handle_file    : FileHandler
        self.handle_dir     : FileHandler
        self.handle_symlink : FileHandler

        self._print_lock           = threading.Lock()
        self._hash_tree_write_lock = threading.Lock()
        self._file_write_lock      = threading.Lock()
        self.output_path           = Path(snapshots_dir).resolve()
        self.cache_path            = self.output_path / f'.cacheback.d'
        self.snapshot_name         = snapshot_name
        self.blob_store_path       = self.cache_path / 'blob'
        self.snapshot_path         = self.output_path / snapshot_name
        self.target_paths          = set(Path(p).expanduser().resolve() for p in target_paths)
        self.exclude_patterns      = set(exclude_patterns)
        self.hash_cache_path       = self.cache_path / 'hash_tree_cache.json'
        self._hash_fn              = getattr(hashlib, hash_algorithm, hashlib.blake2b)
        self.hash_tree             = {}
        self.threads               = threads
        self._thread_outputs       = {}


        self.exclude_patterns.add(f"{self.output_path}")

        if dry_mode:
            self.handle_file    = self._handle_file_dry
            self.handle_dir     = self._handle_dir_dry
            self.handle_symlink = self._handle_symlink_dry
        else:
            self.handle_file    = self._handle_file
            self.handle_dir     = self._handle_dir
            self.handle_symlink = self._handle_symlink

        if verbose:
            self._verbose = True
            self.update_thread_status = self._update_thread_status_verbose
            self.format_file_status = self._format_file_status_verbose
        else:
            self._verbose = False
            self.update_thread_status = self._update_thread_status_ephemeral
            self.format_file_status = self._format_file_status_ephemeral

        self._read_terminal_width()
        signal.signal(signal.SIGWINCH, self._read_terminal_width)


    def _read_terminal_width(self, *_):
        with self._print_lock:
            self._terminal_width = shutil.get_terminal_size((80,0)).columns


    def printerr(self, *args, **kwargs):
        kwargs['file']=sys.stderr
        print(*args, **kwargs)


    def _count_wrapped_lines(self, text: str):
        return (len(text)-1) // self._terminal_width


    def print_ephemeral(self, text: str=''):
        lines = text.splitlines()
        rows = (len(lines)-1) + sum(map(self._count_wrapped_lines, lines))
        if rows > 0:
            lines_up = f'\033[{rows}F'
        else:
            lines_up = ''
        sys.stderr.write(f'\033[0J{text}\033[0G{lines_up}')
        sys.stderr.flush()


    def print_persistent(self, text: str=''):
        with self._print_lock:
            sys.stdout.write(f"\033[0G\033[0J{text}\n")
            sys.stdout.flush()


    def target_iter(self) -> Generator[Path]:
        for target in self.target_paths:
            yield from self.path_recursive_iter(target)


    def is_path_excluded(self, path: Path) -> bool:
        return any(path.full_match(xpat) for xpat in self.exclude_patterns)


    def path_recursive_iter(self, path: Path) -> Generator[Path]:
        if self.is_path_excluded(path):
            self.print_persistent(f"Excluding {path}")
            return
        yield path
        if path.is_dir() and not path.is_symlink():
            for p in path.iterdir():
                yield from self.path_recursive_iter(p)


    def compute_file_hash(self, path: Path) -> str:
        return self._hash_fn(path.read_bytes()).digest().hex()


    @staticmethod
    def get_file_modified_time(path: Path) -> int:
        return int(path.stat().st_mtime)


    @staticmethod
    def get_nested_hash_path(file_hash:      str,
                             dir_delimiter:  str=os.path.sep,
                             nesting_levels: int=2,
                             nest_length:    int=3) -> str:
        _hash = file_hash
        dirs = []
        for i in range(nesting_levels):
            nest_dir = _hash[:nest_length]
            dirs.append(nest_dir)
            _hash = _hash[nest_length:]
        dirs.append(_hash)
        hash_path = dir_delimiter.join(dirs)
        return hash_path


    def load_hash_tree_cache(self) -> None:
        if not self.hash_cache_path.exists():
            return
        with self.hash_cache_path.open('r') as cache_fd:
            hash_tree_cached = json.load(cache_fd)

        def _recursive_path_reconstruct_iter(parent: Path, tree_node: dict):
            for k, v in tree_node.items():
                path = parent / k
                if isinstance(v, dict):
                    yield from _recursive_path_reconstruct_iter(path, v)
                else:
                    yield (path, [*v, False])

        self.hash_tree = {
            str(path): path_props
            for path, path_props in _recursive_path_reconstruct_iter(Path('/'), hash_tree_cached)
        }


    def store_hash_tree_cache(self) -> None:
        hash_tree_nested = {}

        for path_str, props in self.hash_tree.items():
            path_mtime, path_hash, path_visited = props
            if not path_visited:
                self.print_persistent(f"Pruning hash cache entry for {path_str}")
                continue
            path = Path(path_str)
            nested_node = hash_tree_nested
            for part in path.parent.parts[1:]:
                if part not in nested_node:
                    nested_node[part] = {}
                nested_node = nested_node[part]
            nested_node[path.name] = [path_mtime, path_hash]

        with self.hash_cache_path.open('w') as cache_fd:
            json.dump(hash_tree_nested, cache_fd, separators=(',', ':'))


    def query_hash_tree_cache(self, file_path: Path) -> str:
        current_mtime = self.get_file_modified_time(file_path)
        file_hash = None
        if (cached_props := self.hash_tree.get(str(file_path))):
            cached_mtime, cached_hash, visited = cached_props
            if cached_mtime == current_mtime:
                file_hash = cached_hash
        if not file_hash:
            file_hash = self.compute_file_hash(file_path)
        with self._hash_tree_write_lock:
            self.hash_tree[str(file_path)] = [current_mtime, file_hash, True]
        return file_hash


    def _handle_file_dry(self, file: Path, destination: Path) -> None:
        file_hash = self.query_hash_tree_cache(file)
        self.update_thread_status(self.format_file_status(file, file_hash))


    def _handle_file(self, file: Path, destination: Path) -> None:
        file_hash = self.query_hash_tree_cache(file)
        nested_hash_path = self.get_nested_hash_path(file_hash)

        blob_path = self.blob_store_path / nested_hash_path
        blob_path.parent.mkdir(parents=True, exist_ok=True)

        if blob_path.exists():
            ...
        else:
            self.set_thread_status(f"Waiting to copy {file}...")
            with self._file_write_lock:  # Multiple threads writing makes everything slower :)
                if file.stat().st_size > 100_000_000:
                    self.copy_file_with_progress(file, blob_path)
                else:
                    shutil.copy2(file, blob_path)

        self.update_thread_status(self.format_file_status(file, file_hash))

        destination.parent.mkdir(exist_ok=True, parents=True)

        if destination.exists():
            if self._verbose:
                if destination.samefile(blob_path):
                    self.print_persistent(f"{file} is already linked to {file_hash} in this snapshot. The most"
                             " likely cause of this is that a symlink captured in this snapshot points"
                             " to this path.")
                else:
                    self.print_persistent(f"{file} already exists in this snapshot but it does not target the blob"
                             f" in the cache that matches its current hash {file_hash}. Something"
                             " unexpected has occured, dropping into and interactive debugger.")
                    breakpoint()
        else:
            destination.hardlink_to(blob_path)
        return


    def _handle_dir_dry(self, directory: Path, destination: Path) -> None:
        self.update_thread_status(f"DRY: directory {directory}")


    def _handle_dir(self, directory: Path, destination: Path) -> None:
        destination.mkdir(parents=True, exist_ok=True)
        return


    def _handle_symlink_dry(self, symlink: Path, destination: Path) -> None:
        self.update_thread_status(f"DRY: symlink {symlink} => {symlink.readlink()}")


    def _handle_symlink(self, symlink: Path, destination: Path) -> None:
        if destination.exists():
            return
        symlink_destination = symlink.readlink()
        if symlink_destination.is_absolute():
            # re-root the absolute symlink's target
            snapshot_symlink_destination = Path(f"{self.snapshot_path}/{symlink_destination}")
        else:
            # relative symlink will work as expected under the snapshot dir, just duplicate it
            snapshot_symlink_destination = symlink_destination
            symlink_destination = (symlink.parent / symlink_destination).resolve()

        self.update_thread_status(f"Symlink: {symlink} => {symlink_destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            destination.symlink_to(snapshot_symlink_destination)
        except FileExistsError:
            self.print_persistent(f"symlink {snapshot_symlink_destination} already exists")

        if not self.is_path_excluded(symlink_destination):
            self.snapshot_by_type(symlink_destination)
        else:
            self.print_persistent(
                f"Symlink {symlink} points to excluded file {symlink_destination}"
            )
        return


    def handle_unexpected(self, path: Path, destination: Path) -> None:
        if path.exists():
            breakpoint(header=f"Unhandled type for {path}")


    def snapshot_by_type(self, path: Path):
        destination = Path(f"{self.snapshot_path}/{path}")
        if path.is_symlink():
            return self.handle_symlink(path, destination)
        if path.is_dir():
            return self.handle_dir(path, destination)
        if path.is_file():
            return self.handle_file(path, destination)
        if any((path.is_fifo(), path.is_socket(), path.is_char_device(), path.is_block_device())):
            return
        else:
            return self.handle_unexpected(path, destination)


    def take_snapshot(self) -> None:
        self.blob_store_path.mkdir(parents=True, exist_ok=True)
        self.snapshot_path.mkdir(parents=True, exist_ok=False)
        self.print_persistent(f"Creating snapshot {self.snapshot_name}")
        self.load_hash_tree_cache()
        with ThreadPoolExecutor(max_workers=self.threads) as pool:
            pool.map(self.snapshot_by_type, self.target_iter())
        self.store_hash_tree_cache()
        self.print_persistent()


    @staticmethod
    def _copy_bytes(fsrc, fdest, callback, total, length):
        copied = 0
        while True:
            buf = fsrc.read(length)
            if not buf:
                break
            fdest.write(buf)
            copied += len(buf)
            callback(copied, total)


    def _copy_file_with_callback(self,
                                 source_path: Path,
                                 destination_path: Path,
                                 callback: callable,
                                 callback_batch_size=65536):
        size = os.stat(source_path).st_size
        with open(source_path, "rb") as fsrc:
            with open(destination_path, "wb") as fdest:
                self._copy_bytes(fsrc,
                                 fdest,
                                 callback=callback,
                                 total=size,
                                 length=callback_batch_size)
        shutil.copymode(str(source_path), str(destination_path))


    def make_progress_printer(self, label: str) -> Callable[[int,int], str]:
        template = f"{label} [{{filled}}{{empty}}]"
        template_size = len(template.format(filled='',empty=''))
        template_ansi = f"\033[0J{template}\033[0G"

        def _progress_printer(current: int, total: int):
            bar_width = self._terminal_width - template_size
            progress = (current * bar_width) // total
            filled = '-' * progress
            empty = ' ' * (bar_width - progress)
            return template.format(filled=filled, empty=empty)

        return _progress_printer


    def copy_file_with_progress(self, source_path: Path, destination_path: Path) -> None:
        _file_progress = self.make_progress_printer(str(source_path))
        if self._verbose:
            _callback = lambda x,y: self.printerr(_file_progress(x,y), end='\r')
        else:
            _callback = lambda x,y: self.update_thread_status(_file_progress(x,y))
        _callback(0,1)
        self._copy_file_with_callback(source_path, destination_path, _callback)
        self.print_persistent(f"\r\033[0JCopied {source_path}")


    def garbage_collect_blob_cache(self, threads=1) -> None:
        blobs = self.blob_store_path.rglob('*')
        thread_stop_event = threading.Event()
        blob_queue = queue.SimpleQueue()
        blobs_checked = 0
        blobs_total = 1
        _gc_progress = self.make_progress_printer("Garbage collecting")
        self.printerr(_gc_progress(0, 1), end='\r')

        def _check_blob_queue_thread_task():
            nonlocal blobs_checked, blobs_total
            while True:
                try:
                    blob = blob_queue.get(timeout=1)
                    if blob.is_file():
                        if blob.stat().st_nlink < 2:
                            # If blob isn't hardlinked to, delete it from storage
                            self.print_persistent(f"Garbage collecting {blob}")
                            blob.unlink()
                            blob_queue.put(blob.parent)
                            blobs_total += 1
                    elif blob.is_dir():
                        if not any(blob.iterdir()):
                            self.print_persistent(f"Garbage collecting {blob}/")
                            blob.rmdir()
                            blob_queue.put(blob.parent)
                    blobs_checked += 1
                except queue.Empty:
                    if thread_stop_event.is_set():
                        return

        pool = ThreadPoolExecutor(max_workers=threads)
        futures = [pool.submit(_check_blob_queue_thread_task) for i in range(threads)]
        for blob in blobs:
            blobs_total += 1
            blob_queue.put(blob)
            self.printerr(_gc_progress(blobs_checked, blobs_total), end='\r')
        thread_stop_event.set()
        pool.shutdown(wait=True)

        self.print_persistent()


    def print_thread_statuses_ephemeral(self) -> None:
        with self._print_lock:
            dead_threads = (set(self._thread_outputs.keys())
                            .difference(t.ident for t in threading.enumerate()))
            for thread_id in dead_threads:
                self._thread_outputs.pop(thread_id, None)
            body = '\n'.join(self._thread_outputs.values())
            self.print_ephemeral(body)


    def set_thread_status(self, text: str) -> None:
        self._thread_outputs[threading.get_ident()] = text


    def _update_thread_status_ephemeral(self, text: str) -> None:
        self.set_thread_status(text)
        self.print_thread_statuses_ephemeral()


    def _update_thread_status_verbose(self, text: str) -> None:
        with self._print_lock:
            print(text)


    _hash_prefix = " ↳ "
    _hash_prefix_size = len(_hash_prefix)
    _truncate_str = "…"
    _truncate_size = len(_truncate_str)

    def _format_file_status_ephemeral(self, file_path: Path, file_hash: str) -> str:
        file_path_str = str(file_path)
        hash_len_delta = len(file_hash) + self._hash_prefix_size - self._terminal_width
        if hash_len_delta > 0:
            file_hash = f"{file_hash[:-(hash_len_delta+self._truncate_size)]}{self._truncate_str}"
        if len(file_path_str)> self._terminal_width:
            path_truncate_size = self._terminal_width - self._truncate_size
            file_path_str = f"{self._truncate_str}{file_path_str[-path_truncate_size:]}"
        return f"{file_path_str}\n{self._hash_prefix}{file_hash}"


    def _format_file_status_verbose(self, file_path: Path, file_hash: str) -> str:
        return f"{file_path} -> {file_hash}"