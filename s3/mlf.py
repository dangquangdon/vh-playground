from fallocate import fallocate
import valohai
import json

BYTES_IN_GIGABYTE = 1024 * 1024 * 1024

def create_file():
    size = int(valohai.parameters("file_size").value)
    output = valohai.outputs().path(f"{size}gb.file")

    with open(output, "w+b") as f:
        fallocate(f, 0, size * BYTES_IN_GIGABYTE)

    metadata = {
        "valohai.alias": valohai.parameters("datum_alias").value,
    }
    metadata_path = valohai.outputs().path(f"{output}.metadata.json")
    with open(metadata_path, "w") as metadata_out:
        json.dump(metadata, metadata_out)



if __name__ == '__main__':
    create_file()