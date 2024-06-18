import time
import torch
from torch.utils.data import DataLoader, Dataset
from transformers import AutoTokenizer, AutoModel
from typing import List

MAX_LENGTH = 81  # This is the 99th percentile of N tokens in job advert sentences :)


class SentenceDataset(Dataset):
    def __init__(self, sentences, tokenizer, max_length=MAX_LENGTH):
        self.sentences = sentences
        self.tokenizer = tokenizer
        self.max_length = max_length

    def __len__(self):
        return len(self.sentences)

    def __getitem__(self, idx):
        return self.tokenizer(
            self.sentences[idx],
            padding="max_length",
            truncation=True,
            max_length=self.max_length,
            return_tensors="pt",
        )


# Mean Pooling - Take attention mask into account for correct averaging
def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[
        0
    ]  # First element of model_output contains all token embeddings
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    )
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
    sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
    return sum_embeddings / sum_mask


# Function to embed sentences
def embed_sentences(
    sentences: List[str],
    model_name: str = "jjzha/jobbert-base-cased",
    batch_size: int = 32,
    device: str = "cuda" if torch.cuda.is_available() else "cpu",
) -> torch.Tensor:
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name).to(device)

    dataset = SentenceDataset(sentences, tokenizer)
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=False)

    all_embeddings = []

    start_time = time.time()
    print(f"Process started at {start_time}")

    with torch.no_grad():
        for batch in dataloader:
            encoded_input = {
                key: val.squeeze().to(device) for key, val in batch.items()
            }
            model_output = model(**encoded_input)
            batch_embeddings = mean_pooling(
                model_output, encoded_input["attention_mask"]
            )
            all_embeddings.append(batch_embeddings.cpu())

    elapsed_time = time.time() - start_time
    print(f"Batch size: {batch_size}, Time taken: {elapsed_time:.2f} seconds")

    return torch.cat(all_embeddings, dim=0)
