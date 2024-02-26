from fallocate import fallocate
import valohai
import json

BYTES_IN_GIGABYTE = 1024 * 1024 * 1024

def create_file():
    output = valohai.outputs().path("10gb.file")

    with open(output, "w+b") as f:
        fallocate(f, 0, 10 * BYTES_IN_GIGABYTE)

    metadata = {
        "valohai.alias": valohai.parameters("datum_alias").value,
    }
    metadata_path = valohai.outputs().path(f"{output}.metadata.json")
    with open(metadata_path, "w") as metadata_out:
        json.dump(metadata, metadata_out)



if __name__ == '__main__':
    create_file()