import argparse
import pathlib
import pandas as pd

# Parse argument variables passed via the CreateDataset processing step
parser = argparse.ArgumentParser()
parser.add_argument("--athena-data", type=str)
args = parser.parse_args()



dataset = pd.read_parquet(args.athena_data, engine='pyarrow')  
train = dataset.sample(frac=0.80, random_state=0)
test = dataset.drop(train.index)

# Write train, test splits to output path
train_output_path = pathlib.Path("/opt/ml/processing/output/train")
test_output_path = pathlib.Path("/opt/ml/processing/output/test")
baseline_path = pathlib.Path("/opt/ml/processing/output/baseline")

train.to_csv(train_output_path / "train.csv", index=False)
# Reset the test
test.reset_index(inplace=True, drop=True)
test.to_csv(test_output_path / "test.csv", index=False, header=False)
# Save baseline with headers
train.to_csv(baseline_path / "baseline.csv", index=False, header=True)
