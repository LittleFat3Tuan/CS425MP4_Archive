from transformers import ViTFeatureExtractor, ViTForImageClassification
import torch
from PIL import Image
import requests
import time

feature_extractor = ViTFeatureExtractor.from_pretrained("google/vit-base-patch16-224")
model = ViTForImageClassification.from_pretrained("google/vit-base-patch16-224")

s = time.time()
with open('img_urls.txt', 'r') as f:
    for _ in range(50):
        url = f.readline().strip()
        #print(url)
        image = Image.open(requests.get(url, stream=True).raw)

        inputs = feature_extractor(image, return_tensors="pt")

        with torch.no_grad():
            logits = model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        print(model.config.id2label[predicted_label])
print(time.time() - s)


        
    