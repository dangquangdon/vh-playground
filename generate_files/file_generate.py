import random
import string
import valohai
import json


def generate_random_file_content(size_in_bytes):
    return "".join(
        random.choices(
            string.ascii_letters + string.digits,
            k=size_in_bytes,
        )
    ).encode("utf-8")


def generate_random_files(num_files, min_size, max_size):
    dataset_name = str(valohai.parameters("dataset_name").value).strip()
    dataset_version = str(valohai.parameters("dataset_version").value).strip()
    metadata = {
        "valohai.dataset-versions": [
            f"dataset://{dataset_name}/{dataset_version}",
        ]
    }
    for i in range(1, num_files + 1):
        # Generate a random file size within the specified range
        file_size = random.randint(min_size, max_size)

        # Generate random content of the determined file size
        content = generate_random_file_content(file_size)

        # Create the file and write the random content to it
        output = valohai.outputs().path(f"random_file_{i}.txt")
        with open(output, "wb") as f:
            f.write(content)

        metadata_path = f"/valohai/outputs/random_file_{i}.metadata.json"
        with open(metadata_path, "w") as outfile:
            json.dump(metadata, outfile)

        print(f"Created {output} with size {file_size} bytes.")

    
if __name__ == "__main__":
    num_files = int(valohai.parameters("number_of_files").value)
    min_file_size = int(valohai.parameters("min_file_size").value)
    max_file_size = int(valohai.parameters("max_file_size").value)

    generate_random_files(num_files, min_file_size, max_file_size)
