BYTES_IN_GIGABYTE = 1024 * 1024 * 1024
def compare_files(file1, file2):
    with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
        while True:
            chunk1 = f1.read(1 * BYTES_IN_GIGABYTE)
            chunk2 = f2.read(1 * BYTES_IN_GIGABYTE)
            if chunk1 != chunk2:
                return False
            if not chunk1:
                # Both files have been read completely and are identical
                return True

if __name__ == '__main__':
    check_content = compare_files(
        "/tmp/tmp.file",
        "/tmp/4gb.file",
    )

    print("MATCH CONTENT: ", check_content)