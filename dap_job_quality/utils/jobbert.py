from sentence_transformers import SentenceTransformer
import time
import torch
from typing import List

MAX_LENGTH = 81  # This is the 99th percentile of N tokens in job advert sentences :)


# Function to embed sentences
def embed_sentences(
    sentences: List[str],
    model_name: str = "jjzha/jobbert-base-cased",
    batch_size: int = 32,
    device: str = "cuda" if torch.cuda.is_available() else "cpu",
):
    start_time = time.time()
    jobbert_model = SentenceTransformer(model_name, device=device)
    jobbert_model.max_seq_length = MAX_LENGTH
    embeddings = jobbert_model.encode(sentences, batch_size=batch_size)

    elapsed_time = time.time() - start_time
    print(f"Batch size: {batch_size}, Time taken: {elapsed_time:.2f} seconds")

    return embeddings
