from sklearn.metrics import confusion_matrix
import numpy as np
import json
import valohai


def generate_random_confusion():   
    actual = np.random.binomial(1,.9,size = 1000)
    predicted = np.random.binomial(1,.9,size = 1000)
    matrix = confusion_matrix(actual, predicted)
    return matrix.tolist()


if __name__ == "__main__":
    iterations = int(valohai.parameters("iterations").value)
    for i in range(iterations):
        print(json.dumps({f"data{i+1}": generate_random_confusion()}))