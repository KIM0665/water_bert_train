import torch
from torch.utils.data import DataLoader, TensorDataset
from transformers import BertTokenizer
from tqdm import tqdm
from sklearn.metrics import classification_report
from bert_model import BertForWaterQuality
from data_utils import preprocess_data, convert_to_text, tokenize_data, make_train_test
from db_utils import load_water_trade_data_bert

# GPU 설정
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# 1️⃣ 데이터 로드
df = load_water_trade_data_bert()

# 2️⃣ 전처리
df, feature_cols, le = preprocess_data(df)

# 3️⃣ 텍스트 변환
df = convert_to_text(df, feature_cols)

# 4️⃣ 토크나이징
tokenizer = BertTokenizer.from_pretrained("bert-base-multilingual-cased")
encodings, labels = tokenize_data(df, tokenizer)

# 5️⃣ Train/Test 분리
train_data, test_data, y_train, y_test = make_train_test(encodings, labels)

train_dataset = TensorDataset(
    train_data["input_ids"], train_data["attention_mask"], y_train
)
test_dataset = TensorDataset(
    test_data["input_ids"], test_data["attention_mask"], y_test
)

train_loader = DataLoader(train_dataset, batch_size=8, shuffle=True)
test_loader = DataLoader(test_dataset, batch_size=8)

# 6️⃣ 모델 초기화
model = BertForWaterQuality(num_labels=len(le.classes_)).to(device)
optimizer = torch.optim.AdamW(model.parameters(), lr=2e-5)

# 7️⃣ 학습 루프
epochs = 3
for epoch in range(epochs):
    model.train()
    total_loss = 0
    for batch in tqdm(train_loader, desc=f"Epoch {epoch+1}"):
        optimizer.zero_grad()
        input_ids, attention_mask, labels = [b.to(device) for b in batch]
        labels = labels.long()  # 👈 추가
        loss, _ = model(input_ids, attention_mask, labels)
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    print(f"Epoch {epoch+1} | Loss: {total_loss/len(train_loader):.4f}")

# 8️⃣ 평가
model.eval()
all_preds, all_labels = [], []
with torch.no_grad():
    for batch in test_loader:
        input_ids, attention_mask, labels = [b.to(device) for b in batch]
        labels = labels.long()  # 👈 추가
        _, logits = model(input_ids, attention_mask)
        preds = torch.argmax(logits, dim=1)
        all_preds.extend(preds.cpu().numpy())
        all_labels.extend(labels.cpu().numpy())

print("\n=== Classification Report ===")
print(classification_report(all_labels, all_preds, target_names=le.classes_))
# 모델 저장
model_save_path = "./water_quality_bert.pt"
torch.save({
    "model_state_dict": model.state_dict(),
    "label_encoder": le.classes_,  # 클래스 정보도 함께 저장
    "tokenizer_name": "bert-base-multilingual-cased"
}, model_save_path)

print(f"✅ 모델이 저장되었습니다: {model_save_path}")
