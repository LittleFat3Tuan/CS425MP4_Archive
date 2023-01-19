import time
from transformers import MarianMTModel, MarianTokenizer


model_name = "Helsinki-NLP/opus-mt-en-fr"
tokenizer = MarianTokenizer.from_pretrained(model_name)
model = MarianMTModel.from_pretrained(model_name)


def translation(src_text):
    translated = model.generate(**tokenizer(src_text, return_tensors="pt", max_length = 16, truncation=True))
    tgt_text = [tokenizer.decode(t, skip_special_tokens=True) for t in translated]
    return tgt_text
            
            

s = time.time()
with open('testT.txt', 'r') as f:
    for _ in range(50):
        line = f.readline().strip()
        #print(url)
        print(translation(line))
print(time.time() - s)