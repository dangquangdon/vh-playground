from sklearn.metrics import confusion_matrix
import numpy as np
import json

    
actual = np.random.binomial(1,.9,size = 1000)
predicted = np.random.binomial(1,.9,size = 1000)
matrix = confusion_matrix(actual, predicted)

print(json.dumps({"data": matrix.tolist()}))