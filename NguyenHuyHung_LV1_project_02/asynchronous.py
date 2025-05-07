import asyncio
import pandas as pd
import aiohttp
import os
import re
from bs4 import BeautifulSoup
from collections import defaultdict

URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"

SUCCESS_DIR = "products_async"
ERROR_DIR = "error_async"
os.makedirs(SUCCESS_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)

def clean_description(description):
    if not description:
        return ""
    soup = BeautifulSoup(description, "html.parser")
    text = soup.get_text(separator=' ', strip=True)
    text = re.sub(r'\s+', ' ', text)
    return text

def save_error_log(error_type, product_id):
    error_file = os.path.join(ERROR_DIR, "error_async_products.txt")
    with open(error_file, "a", encoding="utf-8") as f:
        f.write(f"{product_id}\n")
    print(f"Saved error ID {product_id} to {error_file}")

def save_product_to_file(data_list, index):
    file_path = os.path.join(SUCCESS_DIR, f"products_{index}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=2)
    print(f"Saved {file_path}")

async def get_product_info(session, product_id, retries=3):
    url = URL.format(product_id)
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
                    print(f"Failed {product_id}: Status {response.status} (Attempt {attempt})")
                    if attempt == retries:
                        # Ghi lỗi sau 3 lần thử
                        save_error_log(error_type, product_id)
        except Exception as e:
            error_type = "exception"
            print(f"Exception {product_id}: {e} (Attempt {attempt})")
            if attempt == retries:
                # Ghi lỗi sau 3 lần thử
                save_error_log(error_type, product_id)
        await asyncio.sleep(1)
    return error_type, {"id": product_id}

async def bound_fetch(semaphore, session, product_id):
    async with semaphore:
        return await get_product_info(session, product_id)

async def fetch_product(product_ids):
    semaphore = asyncio.Semaphore(50)
    connector = aiohttp.TCPConnector(limit_per_host=50)
    success_list = []
    file_index = 1

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [bound_fetch(semaphore, session, pid) for pid in product_ids]

        for idx, future in enumerate(asyncio.as_completed(tasks), 1):
            status, result = await future
            if status == "success":
                success_list.append(result)
            if len(success_list) == 1000:
                save_json(success_list, file_index)
                success_list.clear()
                file_index += 1
        if success_list:
            save_json(success_list, file_index)

        total_success = (file_index - 1) * 1000 + len(success_list)
        print(f"Total success: {total_success}")

if __name__ == "__main__":
    df = pd.read_csv("products-0-200000(in).csv")
    data = df.iloc[:, 0].tolist()
    asyncio.run(fetch_product(data))
