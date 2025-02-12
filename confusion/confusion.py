from sklearn.metrics import confusion_matrix
import numpy as np
import json
import valohai
import string
import random
import datetime


def generate_random_confusion():   
    actual = np.random.binomial(1,.9,size = 1000)
    predicted = np.random.binomial(1,.9,size = 1000)
    matrix = confusion_matrix(actual, predicted)
    return matrix.tolist()


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
    iterations = int(valohai.parameters("iterations").value)
    for i in range(iterations):
        print(json.dumps({f"data{i+1}": generate_random_confusion()}))

    name = datetime.datetime.now().isoformat()
    filename = f"confusion-{name}.txt"
    output = valohai.outputs().path(filename)

        # Generate random content of the determined file size
    generate_large_random_bytes(size_in_bytes=10000, output_path=output)