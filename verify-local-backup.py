import os
import os.path
import time
import hashlib
import concurrent.futures

# Constants
source_root = 'D:\\'
dest_root = 'F:\\'
timestamp_filename = 'backup-verification-timestamp'
chunk_size = 1048576
blacklist_tokens = [
    "$RECYCLE.BIN"
]

# Global error dump list...not clean...but whatever
errors = []

# Classes
class PctLogger:
    def __init__(self, num_total):
        self.last_pct_printed = -1
        self.num_processed = 0
        self.num_total = num_total

    def report_progress(self, num_newly_processed):
        self.num_processed += num_newly_processed
        pct_done = round(self.num_processed / self.num_total * 100, 2)
        if pct_done != self.last_pct_printed:
            print(f'{pct_done}% completed ({self.num_processed} out of {self.num_total})')
            self.last_pct_printed = pct_done

# Functions
def check_dir(path):
    if not os.path.isdir(path):
        print(f'"{path}" is not a directory')
        exit()

def is_valid_source_path(path):
    for entry in blacklist_tokens:
        if entry in path:
            return False
    return True

def get_timestamp(path):
    timestamp_path = os.path.join(path, timestamp_filename)
    if os.path.isfile(timestamp_path):
        with open(timestamp_path) as file:
            contents = file.read()
            return float(contents)
    else:
        return float('-inf')

def write_timestamp(path, timestamp):
    timestamp_path = os.path.join(path, timestamp_filename)
    with open(timestamp_path, mode='w') as file:
        file.write(str(timestamp))

def get_file_hash(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as file:
        while chunk := file.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()

def get_file_hashes(source_path, dest_path, executor):
    source_future = executor.submit(get_file_hash, source_path)
    dest_future = executor.submit(get_file_hash, dest_path)
    return source_future.result(), dest_future.result()

def verify_file(source_path, last_run_timestamp, executor):
    # Check if file exists
    path_template = source_path.removeprefix(source_root)
    dest_path = dest_root.strip('\\') + os.sep + path_template.strip('\\')
    if not os.path.isfile(dest_path):
        errors.append(f'"Dest file not found: {dest_path}')
        return

    # Check if modified times differ
    source_timestamp = os.path.getmtime(source_path)
    dest_timestamp = os.path.getmtime(dest_path)
    if source_timestamp != dest_timestamp:
        source_datetime = time.strftime('%c', time.localtime(source_timestamp))
        dest_datetime = time.strftime('%c', time.localtime(dest_timestamp))
        errors.append(f'Differing timestamps:\n\t{source_path}: {source_datetime}\n\t{dest_path}: {dest_datetime}')
        return

    # Compare hashes if the dest timestamp is newer than the last run timestamp.
    # This helps prevent needlessly rerunning hash functions on old files.
    if dest_timestamp > last_run_timestamp :
        source_hash, dest_hash = get_file_hashes(source_path, dest_path, executor)
        if source_hash != dest_hash:
            errors.append(f'Differing hashes:\n\t{source_path}: {source_hash}\n\t{dest_path}: {dest_hash}')

# Main
print('Verifying roots...')
check_dir(source_root)
check_dir(dest_root)

print('Reading timestamps...')
curr_run_timestamp = time.time()
last_run_timestamp = get_timestamp(dest_root)

print('Gathering paths...')
source_paths = []
for root, dirs, files in os.walk(source_root):
    if is_valid_source_path(root):
        for file in files:
            source_path = os.path.join(root, file)
            source_paths.append(source_path)
    else:
        print(f"INFO: Blacklist match on '{root}'")

num_total = len(source_paths)
pct_logger = PctLogger(num_total)
print(f'Processing {num_total} paths...')
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    for source_path in source_paths:
        verify_file(source_path, last_run_timestamp, executor)
        pct_logger.report_progress(1)

if errors:
    print('Errors encountered:')
    print('\n'.join(errors))
else:
    print('No errors found!')

write_timestamp(dest_root, curr_run_timestamp)