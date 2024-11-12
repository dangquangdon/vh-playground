import random
import string
import valohai
import json
import datetime


def generate_large_random_bytes(size_in_bytes, output_path, chunk_size=1024 * 1024):
    charset = string.ascii_letters + string.digits
    with open(output_path, "wb") as f:
        for _ in range(size_in_bytes // chunk_size):
            # Generate chunk-sized random string and write as bytes
            chunk = "".join(random.choices(charset, k=chunk_size))
            f.write(chunk.encode("utf-8"))

        # Write the remainder if size_in_bytes is not a multiple of chunk_size
        remainder = size_in_bytes % chunk_size
        if remainder:
            chunk = "".join(random.choices(charset, k=remainder))
            f.write(chunk.encode("utf-8"))


def generate_random_files(num_files, min_size, max_size):
    dataset_name = str(valohai.parameters("dataset_name").value).strip()
    dataset_version = str(valohai.parameters("dataset_version").value).strip()
    metadata = {
        "valohai.dataset-versions": [
            {
                "uri": f"dataset://{dataset_name}/{dataset_version}",
                "packaging": True,
            }
        ]
    }

    for i in range(1, num_files + 1):
        # Generate a random file size within the specified range
        file_size = random.randint(min_size, max_size)

        # Create the file and write the random content to it
        name = datetime.datetime.now().isoformat()
        filename = f"{name}-random-file.txt"
        output = valohai.outputs().path(filename)

        # Generate random content of the determined file size
        generate_large_random_bytes(size_in_bytes=file_size, output_path=output)

        metadata_path = f"/valohai/outputs/{filename}.metadata.json"
        with open(metadata_path, "w") as outfile:
            json.dump(metadata, outfile)

        print(f"Created {output} with size {file_size} bytes.")


if __name__ == "__main__":
    num_files = int(valohai.parameters("number_of_files").value)
    min_file_size = int(valohai.parameters("min_file_size").value)
    max_file_size = int(valohai.parameters("max_file_size").value)

    generate_random_files(num_files, min_file_size, max_file_size)
