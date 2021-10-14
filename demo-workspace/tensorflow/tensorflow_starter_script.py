import argparse
import json
import os
import pickle
import sys
import boto3

import numpy as np
import pandas as pd
import tensorflow as tf

from sklearn.model_selection import train_test_split
from tensorflow.keras import layers
from tensorflow.keras.layers.experimental import preprocessing
from tensorflow import feature_column


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Hyperparameters are described here
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--train_data_path", type=str, default=os.environ.get("SM_CHANNEL_TRAIN"))
    parser.add_argument("--bucket", type=str)
    parser.add_argument("--object", type=str)

    # SageMaker specific arguments. Defaults are set in the environment variables.
    parser.add_argument("--model_dir", type=str, default=os.environ.get("SM_MODEL_DIR"))
    parser.add_argument("--output-data-dir", type=str, default=os.environ.get("SM_OUTPUT_DATA_DIR"))

    args = parser.parse_args()
    
    model_dir = os.environ.get("SM_MODEL_DIR")
    
    s3_client = boto3.client("s3")

    data = pd.read_csv(f"{args.train_data_path}/train.csv")
    train_ds = data.copy()
    train_label = train_ds.pop('fraud')
    train_ds = np.array(train_ds)
    
    model = tf.keras.Sequential([
        layers.Dense(16, kernel_regularizer=tf.keras.regularizers.l2(0.01), activation='relu'),
        layers.Dense(64, kernel_regularizer=tf.keras.regularizers.l2(0.01), activation='relu'),
        layers.Dropout(0.2),

        layers.Dense(1, activation='sigmoid')
    ])

    model.compile(optimizer='adam',
                      loss='binary_crossentropy',
                      metrics=['accuracy'])

    results = model.fit(train_ds, 
                        train_label, 
                        validation_split=0.2, 
                        epochs=args.epochs)
    
    metrics_data = {
        "binary_classification_metrics": {
            "train:accuracy": {
                "value": results.history['accuracy'][-1],
                "standard_deviation" : "NaN"
            },
        }
    }

    # Save the model to the location specified by ``model_dir``
    metrics_location = args.output_data_dir + "/metrics.json"
    model_location = model_dir + "/export/Servo/123"

    with open(metrics_location, "w") as f:
        json.dump(metrics_data, f)
        
    s3_client.upload_file(
        Filename=metrics_location, Bucket=args.bucket, Key=args.object
    )
    
    model.save(model_location)
