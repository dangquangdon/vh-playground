import torch
import pandas as pd
import numpy as np
import os
import valohai
import json

from glob import glob

from transformers import pipeline
from nltk import sent_tokenize, download as nltk_download

nltk_download("punkt")
nltk_download("punkt_tab")

model_name = "facebook/bart-large-mnli"
device = 0 if torch.cuda.is_available() else "cpu"


def load_model(device):
    return pipeline(
        task="zero-shot-classification",
        model=model_name,
        device=device,
    )


def load_subtitles_files(files_path):
    file_paths = sorted(glob(files_path))
    scripts = []
    episodes = []
    for file_path in file_paths:
        with open(file_path, "r") as f:
            lines = f.readlines()
            lines = lines[27:]
            lines = [",".join(line.split(",")[9:]) for line in lines]
            lines = [line.replace("\\N", " ").strip() for line in lines]

        script = " ".join(lines)
        scripts.append(script)

        episode = int(file_path.split("-")[-1].split(".")[0].strip())
        episodes.append(episode)

    return pd.DataFrame.from_dict(
        {
            "episodes": episodes,
            "script": scripts,
        }
    )


def unzip_data_dir(path_to_zip_file):
    return os.path.dirname(path_to_zip_file)
    

def get_theme_inferences(script):
    sentences = sent_tokenize(script)
    sentences_batch_size = 20
    batches = []
    for idx in range(0, len(sentences), sentences_batch_size):
        sent = " ".join(sentences[idx : idx + sentences_batch_size])
        batches.append(sent)

    theme_output = theme_classifer(batches, theme_list, multi_label=True)
    themes = {}
    for output in theme_output:
        for label, score in zip(output["labels"], output["scores"]):
            if label not in themes:
                themes[label] = []
            themes[label].append(score)
    return {key: np.mean(np.array(value)) for key, value in themes.items()}


if __name__ == "__main__":
    input_zipfile = valohai.inputs('subtitles').path()
    example_size = int(valohai.parameters('example_size').value)
    output = valohai.outputs().path("classified_themes.csv")

    theme_classifer = load_model(device)

    theme_list = [
        "friendship",
        "hope",
        "sacrifice",
        "battle",
        "self development",
        "betrayal",
        "love",
        "dialogue",
    ]

    subtitle_dir = unzip_data_dir(input_zipfile)
    df = load_subtitles_files(f"{subtitle_dir}/*.ass")
    if example_size:
        df = df.head(example_size)
    
    output_thems = df["script"].apply(get_theme_inferences)
    themes_df = pd.DataFrame(output_thems.tolist())
    themes_df.to_csv(output, encoding='utf-8', index=False)

    metadata_path = valohai.outputs().path(f"{output}.metadata.json")
    with open(metadata_path, "w") as metadata_out:
        json.dump(themes_df.to_dict("index"), metadata_out)

    with valohai.logger() as logger:
        for key, val in themes_df.to_dict("index").items():
            logger.log(key, val)
