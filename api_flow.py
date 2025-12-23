"""
Sequential API Flow Script
Supports base64 decoding between calls and is designed for future parallelization.
"""

import base64
import json
import requests
from typing import Any, Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed


@dataclass
class APIConfig:
    """Configuration for API calls - define your endpoints, headers, and payloads here."""
    base_url: str = "https://api.example.com"
    
    # Define your headers here
    headers: dict = field(default_factory=lambda: {
        "Content-Type": "application/json",
        # "Authorization": "Bearer <token>",
        # "X-Signature": "<signature>",
    })
    
    # Add any default timeout
    timeout: int = 30


class APIFlow:
    """
    Handles sequential API calls with data extraction and transformation between calls.
    """
    
    def __init__(self, config: Optional[APIConfig] = None):
        self.config = config or APIConfig()
        self.session = requests.Session()
        self.session.headers.update(self.config.headers)
        
        # Store data extracted from responses for use in subsequent calls
        self.context: dict[str, Any] = {}
    
    def b64_decode(self, encoded_string: str) -> str:
        """Decode a base64 encoded string."""
        return base64.b64decode(encoded_string).decode('utf-8')
    
    def b64_decode_json(self, encoded_string: str) -> dict:
        """Decode a base64 encoded JSON string and parse it."""
        decoded = self.b64_decode(encoded_string)
        return json.loads(decoded)
    
    def b64_encode(self, data: str) -> str:
        """Encode a string to base64."""
        return base64.b64encode(data.encode('utf-8')).decode('utf-8')
    
    def extract_from_response(self, response: requests.Response, key_path: str) -> Any:
        """
        Extract a value from JSON response using dot notation.
        Example: extract_from_response(resp, "data.user.token")
        """
        data = response.json()
        keys = key_path.split('.')
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key)
            elif isinstance(data, list) and key.isdigit():
                data = data[int(key)]
            else:
                return None
        return data
    
    def make_request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> requests.Response:
        """
        Make an HTTP request.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            endpoint: API endpoint (will be appended to base_url)
            payload: Request body (for POST/PUT)
            headers: Additional headers to merge with defaults
            params: Query parameters
        """
        url = f"{self.config.base_url}{endpoint}"
        
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        response = self.session.request(
            method=method,
            url=url,
            json=payload,
            headers=request_headers,
            params=params,
            timeout=self.config.timeout,
        )
        
        return response
    
    def run_sequential_flow(self) -> dict:
        """
        Define your sequential API call flow here.
        Each step can extract data from previous responses and use it in subsequent calls.
        
        Returns a dict with results/status of the flow.
        """
        results = {}
        
        try:
            # ========================================
            # STEP 1: First API Call
            # ========================================
            print("Step 1: Making first API call...")
            
            # Define your payload for step 1
            step1_payload = {
                # "key": "value",
            }
            
            response1 = self.make_request(
                method="POST",  # or GET, PUT, etc.
                endpoint="/your/endpoint/1",
                payload=step1_payload,
            )
            response1.raise_for_status()
            results['step1'] = response1.json()
            
            # Extract and decode data for next step
            # Example: encoded_data = self.extract_from_response(response1, "data.encoded_token")
            # decoded_data = self.b64_decode(encoded_data)
            # self.context['decoded_token'] = decoded_data
            
            print(f"Step 1 complete. Status: {response1.status_code}")
            
            # ========================================
            # STEP 2: Second API Call (uses data from Step 1)
            # ========================================
            print("Step 2: Making second API call...")
            
            step2_payload = {
                # Use decoded data from step 1
                # "token": self.context.get('decoded_token'),
            }
            
            response2 = self.make_request(
                method="POST",
                endpoint="/your/endpoint/2",
                payload=step2_payload,
            )
            response2.raise_for_status()
            results['step2'] = response2.json()
            
            print(f"Step 2 complete. Status: {response2.status_code}")
            
            # ========================================
            # STEP 3: Third API Call
            # ========================================
            print("Step 3: Making third API call...")
            
            step3_payload = {
                # Build your payload
            }
            
            response3 = self.make_request(
                method="POST",
                endpoint="/your/endpoint/3",
                payload=step3_payload,
            )
            response3.raise_for_status()
            results['step3'] = response3.json()
            
            print(f"Step 3 complete. Status: {response3.status_code}")
            
            # Add more steps as needed...
            
            results['success'] = True
            
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            results['success'] = False
            results['error'] = str(e)
        
        return results


def run_single_thread():
    """Run a single sequential flow."""
    config = APIConfig(
        base_url="https://api.example.com",
        headers={
            "Content-Type": "application/json",
            # Add your headers here
        },
    )
    
    flow = APIFlow(config)
    return flow.run_sequential_flow()


def run_parallel_threads(num_threads: int = 5):
    """
    Run multiple sequential flows in parallel.
    Each thread runs its own independent sequential flow.
    """
    results = []
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(run_single_thread) for _ in range(num_threads)]
        
        for i, future in enumerate(as_completed(futures)):
            try:
                result = future.result()
                results.append({'thread': i, 'result': result})
                print(f"Thread {i} completed: {result.get('success', False)}")
            except Exception as e:
                results.append({'thread': i, 'error': str(e)})
                print(f"Thread {i} failed: {e}")
    
    return results


if __name__ == "__main__":
    # Run a single sequential flow
    print("=" * 50)
    print("Running single thread...")
    print("=" * 50)
    result = run_single_thread()
    print(f"Result: {result}")
    
    # Uncomment below to run parallel threads
    # print("\n" + "=" * 50)
    # print("Running parallel threads...")
    # print("=" * 50)
    # results = run_parallel_threads(num_threads=5)
    # print(f"All results: {results}")

