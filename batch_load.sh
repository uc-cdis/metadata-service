export AGG_MDS_NAMESPACE=mc2dp
export PYTHONPATH=./src
export USE_AGG_MDS=true

CONFIG="${1}"
NUM_LOOPS="${2:-10}"
OFFSET_SIZE="${3:-100}"

if [ -z "$CONFIG" ]; then
  echo "Usage: $0 <config_file> [num_loops] [offset]"
  echo "Update the batch value in the config file to match the offset"
  exit 1
fi

# run the first batch to clear the old indices
python src/mds/populate.py --config "$CONFIG"

# run each batch
for i in $(seq 1 $((NUM_LOOPS - 1))); do
  offset=$((i * OFFSET_SIZE))
  python src/mds/populate.py --config "$CONFIG" --offset $offset --append
done
