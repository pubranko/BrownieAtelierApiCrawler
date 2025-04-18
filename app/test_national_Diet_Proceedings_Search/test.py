import requests
import json
from datetime import datetime, timedelta

class NationalDietProceedingsSearch:
    BASE_URL = "https://kokkai.ndl.go.jp/api/meeting"

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def fetch_data(self):
        current_date = self.start_date
        all_data = []

        while current_date <= self.end_date:
            params = {
                'from': current_date.strftime('%Y-%m-%d'),
                'until': current_date.strftime('%Y-%m-%d'),
                'recordPacking': 'json'
            }
            response = requests.get(self.BASE_URL, params=params)
            # print(response)
            if response.status_code == 200:
                data = response.json()
                all_data.append(data)
            else:
                print(f"Failed to fetch data for {current_date.strftime('%Y-%m-%d')}")
            
            current_date += timedelta(days=1)

        return all_data

# 使用例
if __name__ == "__main__":
    # start_date = datetime.strptime('1980-01-01', '%Y-%m-%d')
    # end_date = datetime.strptime('1980-12-31', '%Y-%m-%d')
    start_date = datetime.strptime('2025-02-26', '%Y-%m-%d')
    end_date = datetime.strptime('2025-02-27', '%Y-%m-%d')
    ndps = NationalDietProceedingsSearch(start_date, end_date)
    data = ndps.fetch_data()
    # print(json.dumps(data, ensure_ascii=False, indent=2))
    
    # JSONファイルに保存
    output_file = "api_crawl/national_Diet_Proceedings_Search/national_diet_proceedings.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Data has been saved to {output_file}")