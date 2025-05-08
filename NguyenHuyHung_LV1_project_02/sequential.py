import requests
import json
import os
import re
import time
import pandas as pd
from bs4 import BeautifulSoup

URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

SUCCESS_DIR = "products_seq"
ERROR_DIR = "errors_seq"
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
    error_file = os.path.join(ERROR_DIR, f"{error_type}.txt")
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
                print(f"Success fetch: {product_id}")
                return "success", product_info
            else:
                error_type = f"status_{response.status_code}"
                print(f"Failed {product_id}: Status {response.status_code} (Attempt {attempt})")
        except Exception as e:
            error_type = "exception"
            print(f"Exception {product_id}: {e} (Attempt {attempt})")
        time.sleep(1)

    save_errors(error_type, product_id)
    return ("unknow_error", {"id": product_id})

def fetch_product(product_ids):
    success_product = []
    error_products = {}
    file_index = 1

    for idx, pid in enumerate(product_ids, 1):
        status, result = get_product_info(pid)
        if status == "success":
            success_product.append(result)
        else:
            error_products[status] = error_products.get(status, 0) + 1

            if len(success_product) == 1000:
                save_product_to_file(success_product, file_index)
                success_product.clear()
                file_index += 1

    if success_product:
        save_product_to_file(success_product, file_index)

    total_success = (file_index - 1) * 1000 + len(success_product)
    total_errors = sum(error_products.values())
    print(f"Total success: {total_success}")
    print(f"Total errors: {total_errors}")
    for err_type, count in error_products.items():
        print(f"   - {err_type}: {count}")

if __name__ == "__main__":
    df = pd.read_csv('products-0-200000(in).csv')
    product_ids = df.iloc[:, 0].tolist()
    fetch_product(product_ids)
