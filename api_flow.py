"""
Performance Testing Script

Simulates a user flow: opening puzzle picker -> selecting crossword -> playing through completion.

Usage:
    python api_flow.py --uid vansh              # single run with specific uid
    python api_flow.py --random-uid             # single run with random uid  
    python api_flow.py --parallel 5 --random-uid  # 5 parallel threads
"""

import base64
import json
import re
import time
import random
import string
import requests
from typing import Any, Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed


def generate_random_uid(length: int = 8) -> str:
    """Generate a random alphanumeric user id for testing."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


@dataclass
class APIConfig:
    """Config for the test run - base URL, credentials, timeouts etc."""
    base_url: str = "https://staging.amuselabs.com/pmm/"
    set_param: str = "pm"
    uid: str = "vansh"
    use_random_uid: bool = False
    
    headers: dict = field(default_factory=lambda: {
        "Content-Type": "application/json",
    })
    
    timeout: int = 30
    
    def get_uid(self) -> str:
        if self.use_random_uid:
            return generate_random_uid()
        return self.uid


class APIFlow:
    """
    Main class that runs the API flow.
    Each step builds on data from the previous one.
    """
    
    def __init__(self, config: Optional[APIConfig] = None):
        self.config = config or APIConfig()
        self.session = requests.Session()
        self.session.headers.update(self.config.headers)
        
        # shared state between steps - tokens, ids, etc
        self.context: dict[str, Any] = {}
    
    def b64_decode(self, encoded: str) -> str:
        """Decode base64 string to utf-8."""
        return base64.b64decode(encoded).decode('utf-8')
    
    def b64_decode_json(self, encoded: str) -> dict:
        """Decode base64 string and parse as JSON."""
        return json.loads(self.b64_decode(encoded))
    
    def extract_params_from_html(self, html: str) -> dict:
        """
        Find the <script id="params" type="application/json"> tag in HTML
        and parse its contents as JSON.
        """
        # try both attribute orderings since they can vary
        patterns = [
            r'<script[^>]*type="application/json"[^>]*id="params"[^>]*>(.*?)</script>',
            r'<script[^>]*id="params"[^>]*type="application/json"[^>]*>(.*?)</script>',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                return json.loads(match.group(1).strip())
        
        raise ValueError("couldn't find params script tag in response")
    
    def make_request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> tuple[requests.Response, float]:
        """
        Make HTTP request and return (response, latency_ms).
        Latency is measured from send to response received.
        """
        url = f"{self.config.base_url}{endpoint}"
        
        req_headers = self.session.headers.copy()
        if headers:
            req_headers.update(headers)
        
        start = time.perf_counter()
        
        response = self.session.request(
            method=method,
            url=url,
            json=payload,
            headers=req_headers,
            params=params,
            timeout=self.config.timeout,
        )
        
        latency_ms = (time.perf_counter() - start) * 1000
        
        return response, latency_ms
    
    # =========================================================================
    # STEP 1: Hit the date picker page, grab the loadToken we'll need later
    # =========================================================================
    def step1_date_picker(self) -> dict:
        uid = self.config.get_uid()
        self.context['uid'] = uid
        
        print(f"Step 1: GET /date-picker (uid={uid})")
        
        response, latency = self.make_request(
            method="GET",
            endpoint="date-picker",
            params={"set": self.config.set_param, "uid": uid},
        )
        response.raise_for_status()
        
        # dig into the HTML to find our params
        params_json = self.extract_params_from_html(response.text)
        
        # rawsps is base64 encoded, contains the loadToken we need
        rawsps = params_json.get("rawsps")
        if not rawsps:
            raise ValueError("no rawsps in response")
        
        decoded = self.b64_decode_json(rawsps)
        load_token = decoded.get("loadToken")
        if not load_token:
            raise ValueError("no loadToken in rawsps")
        
        # stash for later steps
        self.context['load_token'] = load_token
        self.context['params_json'] = params_json
        
        print(f"  done in {latency:.1f}ms")
        
        return {'status_code': response.status_code, 'uid': uid, 'latency_ms': latency}
    
    # =========================================================================
    # STEP 2: Use the loadToken to post the picker status
    # =========================================================================
    def step2_post_picker_status(self) -> dict:
        load_token = self.context.get('load_token')
        if not load_token:
            raise ValueError("need loadToken from step1")
        
        print("Step 2: POST /postPickerStatus")
        
        response, latency = self.make_request(
            method="POST",
            endpoint="postPickerStatus",
            payload={
                "loadToken": load_token,
                "isVerified": True,
                "adDuration": 0,
                "reason": "displaying puzzle picker",
            },
        )
        response.raise_for_status()
        
        data = response.json()
        if data.get("status") != 0:
            raise ValueError(f"postPickerStatus failed: {data}")
        
        print(f"  done in {latency:.1f}ms")
        
        return {'status_code': response.status_code, 'latency_ms': latency}
    
    # =========================================================================
    # STEP 3: Pick a random crossword and load it
    # =========================================================================
    def _get_crossword_puzzles(self) -> list[dict]:
        """Filter the puzzle list down to just crosswords."""
        params = self.context.get('params_json', {})
        streak = params.get('streakInfo', [])
        
        return [
            item['puzzleDetails']
            for item in streak
            if item.get('puzzleDetails', {}).get('puzzleType') == 'CROSSWORD'
        ]
    
    def step3_load_crossword(self) -> dict:
        load_token = self.context.get('load_token')
        uid = self.context.get('uid')
        
        if not load_token or not uid:
            raise ValueError("need context from step1")
        
        # pick a random crossword from what's available
        puzzles = self._get_crossword_puzzles()
        if not puzzles:
            raise ValueError("no crosswords found")
        
        puzzle = random.choice(puzzles)
        puzzle_id = puzzle.get('puzzleId')
        self.context['selected_puzzle'] = puzzle
        self.context['puzzle_id'] = puzzle_id
        
        print(f"Step 3: GET /crossword (id={puzzle_id}, {len(puzzles)} available)")
        
        src_url = f"{self.config.base_url}date-picker?set={self.config.set_param}&uid={uid}"
        
        response, latency = self.make_request(
            method="GET",
            endpoint="crossword",
            params={
                "id": puzzle_id,
                "set": self.config.set_param,
                "picker": "date-picker",
                "src": src_url,
                "uid": uid,
                "loadToken": load_token,
            },
        )
        response.raise_for_status()
        
        # parse out the play info - rawp has the playId we need
        crossword_params = self.extract_params_from_html(response.text)
        self.context['crossword_params'] = crossword_params
        
        rawp = crossword_params.get('rawp')
        if rawp:
            decoded_play = self.b64_decode_json(rawp)
            self.context['decoded_play'] = decoded_play
            self.context['play_id'] = decoded_play.get('playId')
        
        print(f"  done in {latency:.1f}ms")
        
        return {'status_code': response.status_code, 'puzzle_id': puzzle_id, 'latency_ms': latency}
    
    # =========================================================================
    # STEP 4: Simulate playing the puzzle - 10 POST calls to /api/v1/plays
    #   - first call:  playState=1 (started)
    #   - next 8:      playState=2 (in progress, filling in letters)
    #   - last call:   playState=4 (completed)
    # =========================================================================
    def _generate_state(self, length: int, fill_ratio: float = 0.0) -> tuple[str, str]:
        """
        Build primaryState/secondaryState strings.
        primaryState: '#' for empty, letter for filled
        secondaryState: '0' for empty, '1' for filled
        """
        primary, secondary = [], []
        for _ in range(length):
            if random.random() < fill_ratio:
                primary.append(random.choice(string.ascii_lowercase))
                secondary.append('1')
            else:
                primary.append('#')
                secondary.append('0')
        return ''.join(primary), ''.join(secondary)
    
    def _mutate_state(self, primary: str, secondary: str) -> tuple[str, str]:
        """Randomly flip some positions - simulates user typing/erasing."""
        p, s = list(primary), list(secondary)
        
        # change 1-5 random spots
        num_changes = random.randint(1, min(5, len(p)))
        for pos in random.sample(range(len(p)), num_changes):
            if p[pos] == '#':
                p[pos] = random.choice(string.ascii_lowercase)
                s[pos] = '1'
            else:
                p[pos] = '#'
                s[pos] = '0'
        
        return ''.join(p), ''.join(s)
    
    def _complete_state(self, length: int) -> tuple[str, str]:
        """All cells filled in - puzzle done."""
        primary = ''.join(random.choice(string.ascii_lowercase) for _ in range(length))
        return primary, '1' * length
    
    def step4_post_plays(self) -> dict:
        load_token = self.context.get('load_token')
        uid = self.context.get('uid')
        puzzle_id = self.context.get('puzzle_id')
        decoded_play = self.context.get('decoded_play')
        
        if not all([load_token, uid, puzzle_id]):
            raise ValueError("missing context from earlier steps")
        
        play_id = self.context.get('play_id', '')
        
        # figure out grid size for state strings
        puzzle = self.context.get('selected_puzzle', {})
        width = puzzle.get('gridWidth', 5)
        height = puzzle.get('gridHeight', 5)
        state_len = width * height
        
        print(f"Step 4: POST /api/v1/plays (10 iterations, grid={width}x{height})")
        
        # start with a mostly empty grid
        primary, secondary = self._generate_state(state_len, fill_ratio=0.1)
        
        results = []
        
        for i in range(10):
            ts = int(time.time() * 1000)
            
            # determine what state we're in
            if i == 0:
                # just started
                play_state = 1
                curr_primary, curr_secondary = None, None
            elif i == 9:
                # puzzle complete
                play_state = 4
                curr_primary, curr_secondary = self._complete_state(state_len)
            else:
                # still working on it
                play_state = 2
                primary, secondary = self._mutate_state(primary, secondary)
                curr_primary, curr_secondary = primary, secondary
            
            payload = {
                "browser": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
                "fromPicker": "date-picker",
                "getProgressFromBackend": True,
                "id": puzzle_id,
                "inContestMode": False,
                "loadToken": load_token,
                "nClearClicks": 0,
                "nExceptions": 0,
                "nHelpClicks": 0,
                "nPrints": 0,
                "nPrintsEmpty": 0,
                "nPrintsFilled": 0,
                "nPrintsSol": 0,
                "nResizes": 0,
                "nSettingsClicks": 0,
                "playId": play_id,
                "playState": play_state,
                "postScoreReason": "BLUR",
                "primaryState": curr_primary,
                "score": decoded_play.get('score', 0) if decoded_play else 0,
                "secondaryState": curr_secondary,
                "series": self.config.set_param,
                "streakLength": 0,
                "timeOnPage": decoded_play.get('timeOnPage', 5000) if decoded_play else 5000,
                "timeTaken": decoded_play.get('timeTaken', 5) if decoded_play else 5,
                "timestamp": ts,
                "updateLoadTable": False,
                "updatePlayTable": True,
                "updatedTimestamp": ts,
                "userId": uid,
            }
            
            response, latency = self.make_request(
                method="POST",
                endpoint="api/v1/plays",
                payload=payload,
            )
            response.raise_for_status()
            
            data = response.json()
            if data.get("status") != 0:
                raise ValueError(f"plays API failed at iteration {i+1}: {data}")
            
            print(f"  [{i+1}/10] playState={play_state} - {latency:.1f}ms")
            
            results.append({'iteration': i + 1, 'play_state': play_state, 'latency_ms': latency})
        
        print(f"  all done")
        
        return {'iterations': results, 'success': True}
    
    # =========================================================================
    # Main flow - run all steps in sequence
    # =========================================================================
    def run_sequential_flow(self) -> dict:
        """Run the full flow and return results."""
        results = {}
        
        try:
            results['step1'] = self.step1_date_picker()
            results['step2'] = self.step2_post_picker_status()
            results['step3'] = self.step3_load_crossword()
            results['step4'] = self.step4_post_plays()
            results['success'] = True
            
        except requests.exceptions.RequestException as e:
            print(f"request error: {e}")
            results['success'] = False
            results['error'] = str(e)
        except ValueError as e:
            print(f"parse error: {e}")
            results['success'] = False
            results['error'] = str(e)
        
        return results


def run_single_thread(use_random_uid: bool = False, uid: str = "vansh") -> dict:
    """Run one complete flow."""
    config = APIConfig(
        base_url="https://staging.amuselabs.com/pmm/",
        set_param="pm",
        uid=uid,
        use_random_uid=use_random_uid,
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        },
    )
    
    flow = APIFlow(config)
    return flow.run_sequential_flow()


def run_parallel_threads(num_threads: int = 5, use_random_uid: bool = True) -> list[dict]:
    """Spin up multiple threads, each running its own flow."""
    results = []
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(run_single_thread, use_random_uid=use_random_uid)
            for _ in range(num_threads)
        ]
        
        for i, future in enumerate(as_completed(futures)):
            try:
                result = future.result()
                results.append({'thread': i, 'result': result})
                print(f"Thread {i} done: {'OK' if result.get('success') else 'FAILED'}")
            except Exception as e:
                results.append({'thread': i, 'error': str(e)})
                print(f"Thread {i} crashed: {e}")
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Performance Testing")
    parser.add_argument("--random-uid", action="store_true", help="generate random uid for each run")
    parser.add_argument("--uid", type=str, default="vansh", help="uid to use (ignored if --random-uid)")
    parser.add_argument("--parallel", type=int, default=0, help="number of parallel threads (0 = single)")
    
    args = parser.parse_args()
    
    if args.parallel > 0:
        print("=" * 60)
        print(f"Running {args.parallel} parallel threads...")
        print("=" * 60 + "\n")
        
        results = run_parallel_threads(
            num_threads=args.parallel,
            use_random_uid=args.random_uid,
        )
        
    else:
        print("=" * 60)
        print("Running single thread...")
        print("=" * 60 + "\n")
        
        result = run_single_thread(
            use_random_uid=args.random_uid,
            uid=args.uid,
        )
