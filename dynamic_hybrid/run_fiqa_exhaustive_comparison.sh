#!/bin/bash

# Run exhaustive comparison of all weight combinations for FiQA
# This tests the dynamic approach against ALL possible static weight combinations

echo "Starting exhaustive FiQA evaluation with all weight combinations..."

# Run the evaluation with all combinations
python dynamic_hybrid/evaluate_dynamic_hybrid_standalone.py \
    --dataset fiqa \
    --url https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/fiqa.zip \
    --host localhost \
    --port 9200 \
    --index fiqa-index \
    --model-id pCEwOZgBtmNHHhH7ZtEE \
    --output results/fiqa_exhaustive_comparison.json \
    --compare \
    --max-queries 648 \
    --weight-step 0.1

echo "Evaluation complete. Results saved to results/fiqa_exhaustive_comparison.json"

# Extract and display the best static weight
echo ""
echo "=== Finding Best Static Weight ==="
python -c "
import json
with open('results/fiqa_exhaustive_comparison.json', 'r') as f:
    data = json.load(f)
    
# Find best static weight
best_static = None
best_ndcg = 0
for comp in data['comparisons']:
    ndcg = comp['static_results']['average_metrics']['ndcg@10']
    if ndcg > best_ndcg:
        best_ndcg = ndcg
        best_static = comp['static_weights']

dynamic_ndcg = data['dynamic']['average_metrics']['ndcg@10']

print(f'Best static weight: {best_static} (NDCG@10: {best_ndcg:.4f})')
print(f'Dynamic heuristic weight: NDCG@10: {dynamic_ndcg:.4f}')
print(f'Dynamic vs Best Static: {((dynamic_ndcg - best_ndcg) / best_ndcg * 100):+.1f}%')

# Show all static weights ranked
print('\nAll static weights ranked by NDCG@10:')
weights_ranked = []
for comp in data['comparisons']:
    weights_ranked.append((
        comp['static_weights'],
        comp['static_results']['average_metrics']['ndcg@10']
    ))
weights_ranked.sort(key=lambda x: x[1], reverse=True)

for i, (weights, ndcg) in enumerate(weights_ranked[:5]):
    print(f'{i+1}. {weights}: {ndcg:.4f}')
"
