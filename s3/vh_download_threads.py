import valohai
import requests

from requests import Session, Response
import time
import hashlib
import threading

url = str(valohai.parameters('large_file').value or "")
temp_cache_path = "/tmp/tmp.file"



def get_download_size_info(resp: Response) -> tuple[int, int | None]:
    chunk_size = 1048576
    try:
        total: int | None = int(resp.headers["content-length"])
    except (ValueError, TypeError, KeyError):
        total = None
    if total and total > 100 * 1024 * 1024:
        chunk_size = 100 * 1024 * 1024
    return chunk_size, total

# def create_range_headers(chunk_size, total):
#     s = int(total/chunk_size)
#     r = total % chunk_size
#     chunks = []
#     for i in range(s):
#         start = chunk_size * i
#         end = start + chunk_size
#         if i == s - 1:
#             end += r
#         chunks.append((start, end))
#     return chunks

class MultiHasher:
    def __init__(self) -> None:
        self.hashers = {
            'md5': hashlib.md5(),
            'sha1': hashlib.sha1(),
            'sha256': hashlib.sha256(),
        }

    def update(self, chunk: bytes) -> None:
        if not chunk:
            return
        for hasher in self.hashers.values():
            hasher.update(chunk)

    def get_hexdigests(self):
        return {algo: hasher.hexdigest() for (algo, hasher) in self.hashers.items()}


class FileDownloader(threading.Thread):
    def __init__(self, url, start_byte, end_byte, output_file, multihasher):
        super(FileDownloader, self).__init__()
        self.url = url
        self.start_byte = start_byte
        self.end_byte = end_byte
        self.output_file = output_file
        self.multihasher = multihasher

    def run(self):
        headers = {'Range': f'bytes={self.start_byte}-{self.end_byte}'}
        with requests.get(self.url, headers=headers, stream=True) as response:
            response.raise_for_status()
            with open(self.output_file, 'r+b') as f:
                f.seek(self.start_byte)
                chunk_size, total = get_download_size_info(response)
                for chunk in response.iter_content(chunk_size=chunk_size):
                    self.multihasher.update(chunk)
                    f.write(chunk)

def download_file_in_chunks(url, local_filename, num_threads=10):
    multihasher = MultiHasher()
    with requests.get(url, stream=True) as response:
        total_size = int(response.headers.get('content-length', 0))
        if not total_size:
            print("NO SIZE")
            return

    chunk_size = (total_size + num_threads - 1) // num_threads
    threads = []
    with open(local_filename, 'wb') as _:
        for i in range(num_threads):
            start_byte = i * chunk_size
            end_byte = min((i + 1) * chunk_size - 1, total_size - 1)
            thread = FileDownloader(url, start_byte, end_byte, local_filename, multihasher)
            thread.start()
            print(f"Spawning thread....{str(thread)}")
            threads.append(thread)

        for thread in threads:
            print(f"Waiting for thread....{str(thread)}")
            thread.join()

    return multihasher

if __name__ == "__main__":
    start =  time.time()
    multihasher = download_file_in_chunks(
        url=url,
        local_filename=temp_cache_path,
    )
    end = time.time()
    print("Finnish in: ", end-start)
    
    start =  time.time()
    checksums = multihasher.get_hexdigests()
    end = time.time()
    print("calculated checksums in: ", end-start)
    print(checksums)
    
