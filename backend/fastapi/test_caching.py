#!/usr/bin/env python3
"""
Test script to verify FastAPI response caching is working correctly.
This script tests the caching implementation by making multiple requests
to cached endpoints and measuring response times.
"""

import asyncio
import time
import httpx
import statistics
from typing import List, Tuple

async def test_endpoint_caching(
    url: str,
    num_requests: int = 5,
    delay_between_requests: float = 0.1
) -> Tuple[List[float], bool]:
    """
    Test an endpoint for caching behavior.

    Args:
        url: The endpoint URL to test
        num_requests: Number of requests to make
        delay_between_requests: Delay between requests in seconds

    Returns:
        Tuple of (response_times, is_cached)
    """
    response_times = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(num_requests):
            start_time = time.time()

            try:
                response = await client.get(url)
                response.raise_for_status()

                end_time = time.time()
                response_time = (end_time - start_time) * 1000  # Convert to milliseconds
                response_times.append(response_time)
                print(f"Request {i+1}: {response_time:.2f}ms - Status: {response.status_code}")

            except Exception as e:
                print(f"Request {i+1} failed: {e}")
                response_times.append(float('inf'))

            if i < num_requests - 1:
                await asyncio.sleep(delay_between_requests)

    # Check if responses are getting faster (indicating caching)
    if len(response_times) >= 3:
        first_response = response_times[0]
        avg_later_responses = statistics.mean(response_times[1:])

        # If later responses are significantly faster, caching is working
        is_cached = avg_later_responses < (first_response * 0.5)  # 50% faster
    else:
        is_cached = False

    return response_times, is_cached

async def main():
    """Main test function."""
    print("🧪 Testing FastAPI Response Caching")
    print("=" * 50)

    # Test endpoints that should be cached
    test_endpoints = [
        ("Analytics Summary", "http://localhost:8000/api/v1/analytics/summary"),
        ("Analytics Trends", "http://localhost:8000/api/v1/analytics/trends"),
        ("Community Stats", "http://localhost:8000/api/v1/community/stats"),
        ("Community Contributors", "http://localhost:8000/api/v1/community/contributors"),
    ]

    for name, url in test_endpoints:
        print(f"\n📊 Testing {name}")
        print("-" * 30)

        try:
            response_times, is_cached = await test_endpoint_caching(url, num_requests=5)

            if response_times:
                valid_times = [t for t in response_times if t != float('inf')]
                if valid_times:
                    avg_time = statistics.mean(valid_times)
                    min_time = min(valid_times)
                    max_time = max(valid_times)

                    print(f"📊 Response times: Avg={avg_time:.2f}ms, Min={min_time:.2f}ms, Max={max_time:.2f}ms")
                    if is_cached:
                        print("✅ CACHING DETECTED: Later responses are significantly faster!")
                    else:
                        print("⚠️  No caching detected - responses not getting faster")
                else:
                    print("❌ All requests failed")
            else:
                print("❌ No responses received")

        except Exception as e:
            print(f"❌ Test failed: {e}")

    print("\n" + "=" * 50)
    print("🎯 Cache Testing Complete")
    print("\n📋 Expected Results:")
    print("- First request: ~200-500ms (database query)")
    print("- Subsequent requests: <50ms (from Redis cache)")
    print("- Status codes: 200 OK")

if __name__ == "__main__":
    print("⚠️  Make sure the FastAPI server is running on http://localhost:8000")
    print("   Run: python -m uvicorn api.main:app --reload")
    print()

    asyncio.run(main())