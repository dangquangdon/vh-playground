import valohai
import hashlib

def calculate_md5(file_path):
    # Calculate the MD5 checksum of the file
    hash_md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

if __name__ == "__main__":
    input_file = valohai.inputs('large_file').path()
    print("INPUT: ")
    print(input_file)
    print("CALCULATE CHECK SUM.... ")
    checksum = calculate_md5(input_file)
    print("CHECKSUM: ", checksum)
