import json
import random
import time
import math
import argparse
import valohai

parser = argparse.ArgumentParser()
parser.add_argument("--epochs", type=int, default=10)
parser.add_argument("--how_random", type=float, default=0.05)
parser.add_argument("--wait_time", type=float, default=0.05)
parsed = parser.parse_args()
end_epoch = parsed.epochs
how_random = parsed.how_random
wait_time = parsed.wait_time


def logMetadata(epoch, loss, accuracy):
    print(
        json.dumps(
            {
                "epoch": epoch,
                "loss": loss,
                "acc": accuracy,
            }
        )
    )


def lerp(a, b, t):
    return a * (1 - t) + b * t


def steep_log01(t):
    return max(0.0, (1 + math.log10(max(0.000000001, t)) / 10))


def random_m1_p1():
    return 2.0 * (random.random() - 0.5)


output = valohai.outputs().path("output.txt")
with open(output, "w") as file:
    file.write("Something is happening....\n")
    for epoch in range(0, end_epoch):
        t = epoch / end_epoch
        logMetadata(
            epoch + 1,
            lerp(4.0, 2.0, t) + random.random() * how_random,
            steep_log01(t) + (how_random * random_m1_p1()),
        )
        sleep_time = wait_time * (0.8 + 0.4 * random.random())
        file.write(f"{sleep_time}\n")
        time.sleep(sleep_time)
        
