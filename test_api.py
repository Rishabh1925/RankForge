import httpx
import time
import json
import sys

def run_test():
    url = "http://localhost:8000/api/v1/analyze-keywords"
    payload = {
        "primary_keyword": "artificial intelligence",
        "target_location": "United States",
        "content_type": "blog"
    }
    
    # Poll until server is up
    for _ in range(10):
        try:
            httpx.get("http://localhost:8000/api/v1/health")
            break
        except Exception:
            time.sleep(1)
            
    print("Testing Keyword Analysis (SERP Gap and Traffic)...")
    try:
        response = httpx.post(url, json=payload, timeout=30.0)
        data = response.json()
        
        print("\n=== PROJECTED TRAFFIC ===")
        print(f"Base Estimated Searches: {data['traffic_projection']['estimated_monthly_searches']}")
        
        print("\n=== SERP GAP (MISSING TOPICS) ===")
        for topic in data['serp_gap']['missing_topics']:
            print(f"- {topic}")
            
        print("\n=== SERP GAP (UNDERSERVED QUESTIONS) ===")
        for q in data['serp_gap']['underserved_questions']:
            print(f"- {q}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_test()
