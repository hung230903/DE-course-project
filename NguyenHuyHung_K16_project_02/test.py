import asyncio
import aiohttp
import json
import os
import re
from bs4 import BeautifulSoup
import pandas as pd
from collections import defaultdict

# API endpoint template
API_TEMPLATE = "https://api.tiki.vn/product-detail/api/v1/products/{}"

# Output folders
SUCCESS_DIR = "products_async"
ERROR_DIR = "error_async"
os.makedirs(SUCCESS_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)

# Chuẩn hóa description
def clean_description(description):
    if not description:
        return ""
    soup = BeautifulSoup(description, "html.parser")
    text = soup.get_text(separator=' ', strip=True)
    text = re.sub(r'\s+', ' ', text)
    return text

# Ghi danh sách sản phẩm lỗi theo từng loại
def save_error_log(error_dict):
    for error_type, products in error_dict.items():
        os.makedirs(os.path.join(ERROR_DIR, error_type), exist_ok=True)
        with open(os.path.join(ERROR_DIR, error_type, "error_async_products.json"), "w", encoding="utf-8") as f:
            json.dump(products, f, ensure_ascii=False, indent=2)

# Ghi dữ liệu thành file json
def save_json(data_list, index):
    file_path = os.path.join(SUCCESS_DIR, f"products_{index}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved {file_path}")

# Fetch product detail (retry 3 lần)
async def fetch_product(session, product_id, retries=3):
    url = API_TEMPLATE.format(product_id)
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    product_info = {
                        "id": data.get("id"),
                        "name": data.get("name"),
                        "url_key": data.get("url_key"),
                        "price": data.get("price"),
                        "description": clean_description(data.get("description")),
                        "images": data.get("images", []),
                    }
                    print(f"✅ Success fetch: {product_id}")
                    return "success", product_info
                else:
                    error_type = f"status_{response.status}"
                    print(f"❌ Failed {product_id}: Status {response.status} (Attempt {attempt})")
        except Exception as e:
            error_type = "exception"
            print(f"❌ Exception {product_id}: {e} (Attempt {attempt})")
        await asyncio.sleep(1)  # Wait before retry
    return error_type, {"id": product_id}

# Worker task
async def bound_fetch(semaphore, session, product_id):
    async with semaphore:
        return await fetch_product(session, product_id)

# Main logic
async def main(product_ids):
    semaphore = asyncio.Semaphore(50)
    connector = aiohttp.TCPConnector(limit_per_host=50)
    success_list = []
    error_dict = defaultdict(list)
    file_index = 1

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [bound_fetch(semaphore, session, pid) for pid in product_ids]

        for idx, future in enumerate(asyncio.as_completed(tasks), 1):
            status, result = await future
            if status == "success":
                success_list.append(result)
            else:
                error_dict[status].append(result)

            # Save mỗi 1000 sản phẩm
            if len(success_list) == 1000:
                save_json(success_list, file_index)
                success_list.clear()
                file_index += 1

        # Save phần còn lại
        if success_list:
            save_json(success_list, file_index)

        # Save lỗi
        save_error_log(error_dict)

        # Summary
        total_success = (file_index - 1) * 1000 + len(success_list)
        total_errors = sum(len(v) for v in error_dict.values())
        print(f"\n🔎 Summary:")
        print(f"✅ Total success: {total_success}")
        print(f"❌ Total errors: {total_errors}")
        for err_type, items in error_dict.items():
            print(f"   - {err_type}: {len(items)}")

if __name__ == "__main__":
    df = pd.read_csv("products-0-200000(in).csv")
    data = df.iloc[:, 0].tolist()
    asyncio.run(main(data))
