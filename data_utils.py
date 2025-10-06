import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import torch
from transformers import BertTokenizer

def preprocess_data(df):
    """
    수질 데이터 전처리
    - 결측치 처리
    - 숫자형 변환
    - 라벨 인코딩
    """
    feature_cols = [c for c in df.columns if c.lower().startswith("m")]
    
    for col in feature_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df[feature_cols] = df[feature_cols].fillna(df[feature_cols].mean())
    
    le = LabelEncoder()
    df["alert_label"] = le.fit_transform(df["alert_level"])
    return df, feature_cols, le


def convert_to_text(df, feature_cols):
    """
    BERT 입력용 텍스트 생성
    """
    def row_to_text(row):
        text = f"{row['swmn_nm']} ({row['detail_adres']}) 수질 데이터: "
        text += ", ".join([f"{col}: {row[col]:.2f}" for col in feature_cols])
        print(text)
        return text
    
    df["text"] = df.apply(row_to_text, axis=1)
    return df


def tokenize_data(df, tokenizer, max_length=256):
    """
    BERT 토크나이징 수행
    """
    encodings = tokenizer(
        df["text"].tolist(),
        truncation=True,
        padding=True,
        max_length=max_length,
        return_tensors="pt"
    )
    labels = torch.tensor(df["alert_label"].values)
    return encodings, labels


def make_train_test(encodings, labels, test_size=0.2):
    """
    학습 / 검증 데이터 분할
    """
    dataset_size = len(labels)
    indices = list(range(dataset_size))
    split = int(dataset_size * (1 - test_size))
    train_indices, test_indices = indices[:split], indices[split:]
    
    train_data = {k: v[train_indices] for k, v in encodings.items()}
    test_data = {k: v[test_indices] for k, v in encodings.items()}
    y_train, y_test = labels[train_indices], labels[test_indices]
    
    return train_data, test_data, y_train, y_test
