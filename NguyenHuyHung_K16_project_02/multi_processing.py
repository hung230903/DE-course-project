import requests
import json
import os
import re
import time
import pandas as pd
from bs4 import BeautifulSoup
from collections import defaultdict
from multiprocessing import Pool, cpu_count

URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

SUCCESS_DIR = "products_mp"
ERROR_DIR = "error_mp"
os.makedirs(SUCCESS_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)

def clean_description(description):
    if not description:
        return ""
    soup = BeautifulSoup(description, "html.parser")
    text = soup.get_text(separator=' ', strip=True)
    text = re.sub(r'\s+', ' ', text)
    return text

def save_errors(error_type, product_id):
    error_file = os.path.join(ERROR_DIR, "error_mp_products.txt")
    with open(error_file, "a", encoding="utf-8") as f:
        f.write(f"{product_id}\n")
    print(f"Saved error ID {product_id} to {error_file}")

def save_product_to_file(data_list, index):
    file_path = os.path.join(SUCCESS_DIR, f"products_{index}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=2)
    print(f"Saved {file_path}")

def get_product_info(product_id, retries=3):
    url = URL.format(product_id)
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                data = response.json()
                product_info = {
                    "id": data.get("id"),
                    "name": data.get("name"),
                    "url_key": data.get("url_key"),
                    "price": data.get("price"),
                    "description": clean_description(data.get("description")),
                    "images": data.get("images", []),
                }
                print(f"✅ Success fetch: {product_id}")
                return ("success", product_info)
            else:
                error_type = f"status_{response.status_code}"
                print(f"Failed {product_id}: Status {response.status_code} (Attempt {attempt})")
                if attempt == retries:
                    save_errors(error_type, product_id)
        except Exception as e:
            error_type = "exception"
            print(f"Exception {product_id}: {e} (Attempt {attempt})")
            if attempt == retries:
                save_errors(error_type, product_id)
        time.sleep(1)

    return (error_type, {"id": product_id})

def fetch_product(product_ids):
    success_list = []
    file_index = 1

    with Pool(processes=cpu_count()) as pool:
        results = pool.map(get_product_info, product_ids)

        for idx, (status, result) in enumerate(results, 1):
            if status == "success":
                success_list.append(result)
            else:
                error_type = result["id"]
                save_errors(status, error_type)

            if len(success_list) == 1000:
                save_product_to_file(success_list, file_index)
                success_list.clear()
                file_index += 1

    if success_list:
        save_product_to_file(success_list, file_index)

    total_success = (file_index - 1) * 1000 + len(success_list)
    print(f"Total success: {total_success}")

if __name__ == "__main__":
    df = pd.read_csv('products-0-200000(in).csv')
    product_ids = df.iloc[:, 0].tolist()
    fetch_product(product_ids)
