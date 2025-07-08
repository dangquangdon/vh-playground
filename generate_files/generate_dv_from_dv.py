import os
import shutil
import valohai
import random
import datetime
import string
import json


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


if __name__ == "__main__":
    dataset_name = str(valohai.parameters("dataset_name").value).strip()
    dataset_version_name = str(valohai.parameters("dataset_version_name").value).strip()
    source_dataset_version_uri = valohai.inputs("dataset").paths()
    number_of_files = int(valohai.parameters("number_of_files").value)
    min_file_size = int(valohai.parameters("min_file_size").value)
    max_file_size = int(valohai.parameters("max_file_size").value)
    packaging = bool(int(valohai.parameters("packaging").value))

    metadata = {
        "valohai.dataset-versions": [
            {
                "uri": f"dataset://{dataset_name}/{dataset_version_name}",
                "packaging": packaging,
                "exclude": ["not_to_copy.exclude",]
            }
        ]
    }

    for file in source_dataset_version_uri:
        basename = os.path.basename(file)
        output_file = valohai.outputs().path(basename)
        shutil.copy(file, output_file)
        metadata_path = f"/valohai/outputs/{basename}.metadata.json"
        with open(metadata_path, "w") as outfile:
            json.dump(metadata, outfile)
        print(f"Copied {file} to {output_file}")


    for _ in range(1, number_of_files + 1):
        # Generate a random file size within the specified range
        file_size = random.randint(min_file_size, max_file_size)

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
