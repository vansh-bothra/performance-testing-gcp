"""
Performance Testing Script - V2 (Hardcoded Puzzle)

Same as api_flow.py but with:
  - Hardcoded puzzle ID: e3d146e8
  - Hardcoded state length: 185

Usage:
    python api_flow_v2.py --uid vansh
    python api_flow_v2.py --random-uid
    python api_flow_v2.py --parallel 5 --random-uid
    python api_flow_v2.py --rps 30 --duration 5 --title "V2 Test" --output results/
"""

import base64
import csv
import json
import os
import re
import time
import random
import string
import requests
from datetime import datetime
from typing import Any, Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed


# hardcoded values for v2
PUZZLE_ID = "1461ef6d"
STATE_LEN = 185


def generate_random_uid(length: int = 8) -> str:
    """Generate a random alphanumeric user id for testing."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


@dataclass
class APIConfig:
    """Config for the test run - base URL, credentials, timeouts etc."""
    base_url: str = "https://cdn-test.amuselabs.com/pmm/"
    set_param: str = "gandalf"
    uid: str = "vansh"
    use_random_uid: bool = False
    uid_pool: list = field(default_factory=list)
    
    headers: dict = field(default_factory=lambda: {
        "Content-Type": "application/json",
    })
    
    timeout: int = 30
    
    def get_uid(self) -> str:
        if self.uid_pool:
            return random.choice(self.uid_pool)
        if self.use_random_uid:
            return generate_random_uid()
        return self.uid


class APIFlow:
    """
    Main class that runs the API flow.
    Each step builds on data from the previous one.
    """
    
    def __init__(self, config: Optional[APIConfig] = None, verbose: bool = False):
        self.config = config or APIConfig()
        self.verbose = verbose
        self.session = requests.Session()
        self.session.headers.update(self.config.headers)
        
        # shared state between steps
        self.context: dict[str, Any] = {}
    
    def _log(self, msg: str):
        """Print only if verbose mode is enabled."""
        if self.verbose:
            print(msg)
    
    def b64_decode(self, encoded: str) -> str:
        """Decode base64 string to utf-8."""
        return base64.b64decode(encoded).decode('utf-8')
    
    def b64_decode_json(self, encoded: str) -> dict:
        """Decode base64 string and parse as JSON."""
        return json.loads(self.b64_decode(encoded))
    
    def extract_params_from_html(self, html: str) -> dict:
        """Find the <script id="params"> tag and parse its JSON."""
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
        """Make HTTP request and return (response, latency_ms)."""
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
    # STEP 1: Hit the date picker page, grab the loadToken
    # =========================================================================
    def step1_date_picker(self) -> dict:
        uid = self.config.get_uid()
        self.context['uid'] = uid
        
        self._log(f"Step 1: GET /date-picker (uid={uid})")
        
        response, latency = self.make_request(
            method="GET",
            endpoint="date-picker",
            params={"set": self.config.set_param, "uid": uid},
        )
        response.raise_for_status()
        
        params_json = self.extract_params_from_html(response.text)
        
        rawsps = params_json.get("rawsps")
        if not rawsps:
            raise ValueError("no rawsps in response")
        
        decoded = self.b64_decode_json(rawsps)
        load_token = decoded.get("loadToken")
        if not load_token:
            raise ValueError("no loadToken in rawsps")
        
        self.context['load_token'] = load_token
        self.context['params_json'] = params_json
        
        self._log(f"  done in {latency:.1f}ms")
        
        return {'status_code': response.status_code, 'uid': uid, 'latency_ms': latency}
    
    # =========================================================================
    # STEP 2: Post picker status
    # =========================================================================
    def step2_post_picker_status(self) -> dict:
        load_token = self.context.get('load_token')
        if not load_token:
            raise ValueError("need loadToken from step1")
        
        self._log("Step 2: POST /postPickerStatus")
        
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
        
        self._log(f"  done in {latency:.1f}ms")
        
        return {'status_code': response.status_code, 'latency_ms': latency}
    
    # =========================================================================
    # STEP 3: Load the hardcoded crossword puzzle
    # =========================================================================
    def step3_load_crossword(self) -> dict:
        load_token = self.context.get('load_token')
        uid = self.context.get('uid')
        
        if not load_token or not uid:
            raise ValueError("need context from step1")
        
        # use hardcoded puzzle ID
        puzzle_id = PUZZLE_ID
        self.context['puzzle_id'] = puzzle_id
        
        self._log(f"Step 3: GET /crossword (id={puzzle_id}) [HARDCODED]")
        
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
        
        crossword_params = self.extract_params_from_html(response.text)
        self.context['crossword_params'] = crossword_params
        
        rawp = crossword_params.get('rawp')
        if rawp:
            decoded_play = self.b64_decode_json(rawp)
            self.context['decoded_play'] = decoded_play
            self.context['play_id'] = decoded_play.get('playId')
        
        self._log(f"  done in {latency:.1f}ms")
        
        return {'status_code': response.status_code, 'puzzle_id': puzzle_id, 'latency_ms': latency}
    
    # =========================================================================
    # STEP 4: Simulate playing - 10 POST calls with hardcoded state length
    # =========================================================================
    def _generate_state(self, length: int, fill_ratio: float = 0.0) -> tuple[str, str]:
        """Build primaryState/secondaryState strings."""
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
        """Randomly flip some positions."""
        p, s = list(primary), list(secondary)
        
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
        """All cells filled in."""
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
        
        # use hardcoded state length
        state_len = STATE_LEN
        
        self._log(f"Step 4: POST /api/v1/plays (10 iterations, state_len={state_len}) [HARDCODED]")
        
        primary, secondary = self._generate_state(state_len, fill_ratio=0.1)
        
        results = []
        
        for i in range(10):
            ts = int(time.time() * 1000)
            
            if i == 0:
                play_state = 1
                curr_primary, curr_secondary = None, None
            elif i == 9:
                play_state = 4
                curr_primary, curr_secondary = self._complete_state(state_len)
            else:
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
            
            self._log(f"  [{i+1}/10] playState={play_state} - {latency:.1f}ms")
            
            results.append({'iteration': i + 1, 'play_state': play_state, 'latency_ms': latency})
        
        self._log(f"  all done")
        
        return {'iterations': results, 'success': True}
    
    # =========================================================================
    # Main flow
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
            self._log(f"request error: {e}")
            results['success'] = False
            results['error'] = str(e)
        except ValueError as e:
            self._log(f"parse error: {e}")
            results['success'] = False
            results['error'] = str(e)
        
        return results


def run_single_thread(use_random_uid: bool = False, uid: str = "vansh", uid_pool: list = None, verbose: bool = False) -> dict:
    """Run one complete flow."""
    config = APIConfig(
        base_url="https://cdn-test.amuselabs.com/pmm/",
        set_param="gandalf",
        uid=uid,
        use_random_uid=use_random_uid,
        uid_pool=uid_pool or [],
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        },
    )
    
    flow = APIFlow(config, verbose=verbose)
    return flow.run_sequential_flow()


def run_parallel_threads(num_threads: int = 5, use_random_uid: bool = True, uid_pool: list = None, verbose: bool = False) -> list[dict]:
    """Spin up multiple threads, each running its own flow."""
    results = []
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(run_single_thread, use_random_uid=use_random_uid, uid_pool=uid_pool, verbose=verbose)
            for _ in range(num_threads)
        ]
        
        for i, future in enumerate(as_completed(futures)):
            try:
                result = future.result()
                results.append({'thread': i, 'result': result})
                if verbose:
                    print(f"Thread {i} done: {'OK' if result.get('success') else 'FAILED'}")
            except Exception as e:
                results.append({'thread': i, 'error': str(e)})
                if verbose:
                    print(f"Thread {i} crashed: {e}")
    
    return results


def run_wave_execution(
    rps: int = 30,
    duration: int = 1,
    use_random_uid: bool = False,
    uid_pool: list = None,
    title: str = "",
    verbose: bool = False,
) -> dict:
    """Run wave-based load test: start `rps` threads every second for `duration` seconds."""
    all_results = []
    waves = []
    
    total_threads = rps * duration
    if verbose:
        print(f"Starting wave execution: {rps} req/sec for {duration} seconds ({total_threads} total)")
        print(f"Using hardcoded puzzle: {PUZZLE_ID}, state_len: {STATE_LEN}")
        print("-" * 60)
    
    overall_start = time.perf_counter()
    
    for wave_num in range(duration):
        wave_start = time.perf_counter()
        if verbose:
            print(f"\nWave {wave_num + 1}/{duration}: Starting {rps} threads...")
        
        wave_results = []
        
        with ThreadPoolExecutor(max_workers=rps) as executor:
            futures = [
                executor.submit(run_single_thread, use_random_uid=use_random_uid, uid_pool=uid_pool, verbose=verbose)
                for _ in range(rps)
            ]
            
            for i, future in enumerate(as_completed(futures)):
                try:
                    result = future.result()
                    wave_results.append({
                        'wave': wave_num + 1,
                        'thread': i,
                        'result': result
                    })
                except Exception as e:
                    wave_results.append({
                        'wave': wave_num + 1,
                        'thread': i,
                        'error': str(e)
                    })
        
        wave_end = time.perf_counter()
        wave_duration_ms = (wave_end - wave_start) * 1000
        
        successful = [r for r in wave_results if r.get('result', {}).get('success')]
        failed = len(wave_results) - len(successful)
        
        latencies = []
        for r in successful:
            res = r['result']
            total = (
                res.get('step1', {}).get('latency_ms', 0) +
                res.get('step2', {}).get('latency_ms', 0) +
                res.get('step3', {}).get('latency_ms', 0) +
                sum(it.get('latency_ms', 0) for it in res.get('step4', {}).get('iterations', []))
            )
            latencies.append(total)
        
        wave_stats = {
            'wave_number': wave_num + 1,
            'threads': len(wave_results),
            'success': len(successful),
            'failed': failed,
            'duration_ms': wave_duration_ms,
            'latencies': latencies,
        }
        
        if latencies:
            wave_stats['min'] = min(latencies)
            wave_stats['max'] = max(latencies)
            wave_stats['avg'] = sum(latencies) / len(latencies)
        
        waves.append(wave_stats)
        all_results.extend(wave_results)
        
        if verbose:
            print(f"  Wave {wave_num + 1} complete: {len(successful)}/{len(wave_results)} success, {wave_duration_ms:.0f}ms")
        elif (wave_num + 1) % 50 == 0:
            print(f"Completed {wave_num + 1}/{duration} waves...")
        
        if wave_num < duration - 1:
            elapsed = time.perf_counter() - wave_start
            sleep_time = max(0, 1.0 - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)
    
    overall_end = time.perf_counter()
    total_time_ms = (overall_end - overall_start) * 1000
    
    return {
        'title': title,
        'timestamp': datetime.now().isoformat(),
        'config': {
            'rps': rps,
            'duration': duration,
            'total_threads': total_threads,
            'puzzle_id': PUZZLE_ID,
            'state_len': STATE_LEN,
        },
        'waves': waves,
        'results': all_results,
        'total_time_ms': total_time_ms,
    }


def save_results_to_csv(run_data: dict, output_path: str) -> str:
    """Save run results to CSV file."""
    title = run_data.get('title', 'run')
    safe_title = re.sub(r'[^\w\s-]', '', title).replace(' ', '_')
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    if output_path.endswith('.csv'):
        filepath = output_path
    else:
        os.makedirs(output_path, exist_ok=True)
        filename = f"{timestamp}_{safe_title}_v2.csv" if safe_title else f"{timestamp}_v2.csv"
        filepath = os.path.join(output_path, filename)
    
    rows = []
    for item in run_data.get('results', []):
        wave = item.get('wave', 0)
        thread = item.get('thread', 0)
        
        if 'error' in item:
            rows.append({
                'wave': wave,
                'thread': thread,
                'uid': '',
                'success': False,
                'error': item['error'],
                'step1_ms': '',
                'step2_ms': '',
                'step3_ms': '',
                'step4_avg_ms': '',
                'step4_total_ms': '',
                'total_ms': '',
            })
            continue
        
        result = item.get('result', {})
        if not result.get('success'):
            rows.append({
                'wave': wave,
                'thread': thread,
                'uid': result.get('step1', {}).get('uid', ''),
                'success': False,
                'error': result.get('error', 'unknown'),
                'step1_ms': '',
                'step2_ms': '',
                'step3_ms': '',
                'step4_avg_ms': '',
                'step4_total_ms': '',
                'total_ms': '',
            })
            continue
        
        s1 = result.get('step1', {})
        s2 = result.get('step2', {})
        s3 = result.get('step3', {})
        s4 = result.get('step4', {})
        
        s4_iters = s4.get('iterations', [])
        s4_latencies = [it.get('latency_ms', 0) for it in s4_iters]
        s4_total = sum(s4_latencies)
        s4_avg = s4_total / len(s4_latencies) if s4_latencies else 0
        
        l1 = s1.get('latency_ms', 0)
        l2 = s2.get('latency_ms', 0)
        l3 = s3.get('latency_ms', 0)
        total = l1 + l2 + l3 + s4_total
        
        rows.append({
            'wave': wave,
            'thread': thread,
            'uid': s1.get('uid', ''),
            'success': True,
            'error': '',
            'step1_ms': round(l1, 2),
            'step2_ms': round(l2, 2),
            'step3_ms': round(l3, 2),
            'step4_avg_ms': round(s4_avg, 2),
            'step4_total_ms': round(s4_total, 2),
            'total_ms': round(total, 2),
        })
    
    fieldnames = ['wave', 'thread', 'uid', 'success', 'error', 'step1_ms', 'step2_ms', 'step3_ms', 'step4_avg_ms', 'step4_total_ms', 'total_ms']
    
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    return filepath


def format_results_table(results: list[dict], total_time_ms: float) -> str:
    """Format results as a table with aggregate stats."""
    lines = []
    
    # header
    lines.append("")
    lines.append("=" * 90)
    lines.append("RESULTS SUMMARY")
    lines.append("=" * 90)
    lines.append("")
    lines.append(f"{'Thread':<8} {'Status':<10} {'UID':<12} {'Step1':<10} {'Step2':<10} {'Step3':<10} {'Step4 Avg':<12} {'Total':<10}")
    lines.append("-" * 90)
    
    all_latencies = []
    step_latencies = {'step1': [], 'step2': [], 'step3': [], 'step4': []}
    success_count = 0
    fail_count = 0
    
    for item in results:
        thread_id = item.get('thread', '-')
        
        if 'error' in item:
            lines.append(f"{thread_id:<8} {'CRASHED':<10} {'-':<12} {'-':<10} {'-':<10} {'-':<10} {'-':<12} {'-':<10}")
            fail_count += 1
            continue
        
        result = item.get('result', {})
        success = result.get('success', False)
        
        if not success:
            uid = result.get('step1', {}).get('uid', '-')
            lines.append(f"{thread_id:<8} {'FAILED':<10} {uid:<12} {'-':<10} {'-':<10} {'-':<10} {'-':<12} {'-':<10}")
            fail_count += 1
            continue
        
        success_count += 1
        
        s1 = result.get('step1', {})
        s2 = result.get('step2', {})
        s3 = result.get('step3', {})
        s4 = result.get('step4', {})
        
        uid = s1.get('uid', '-')[:10]  # truncate long uids
        l1 = s1.get('latency_ms', 0)
        l2 = s2.get('latency_ms', 0)
        l3 = s3.get('latency_ms', 0)
        
        # step4 has multiple iterations, compute average
        s4_iterations = s4.get('iterations', [])
        s4_latencies = [it.get('latency_ms', 0) for it in s4_iterations]
        l4_avg = sum(s4_latencies) / len(s4_latencies) if s4_latencies else 0
        l4_total = sum(s4_latencies)
        
        total_latency = l1 + l2 + l3 + l4_total
        all_latencies.append(total_latency)
        
        step_latencies['step1'].append(l1)
        step_latencies['step2'].append(l2)
        step_latencies['step3'].append(l3)
        step_latencies['step4'].extend(s4_latencies)
        
        lines.append(f"{thread_id:<8} {'OK':<10} {uid:<12} {l1:<10.1f} {l2:<10.1f} {l3:<10.1f} {l4_avg:<12.1f} {total_latency:<10.1f}")
    
    # aggregate stats
    lines.append("-" * 90)
    lines.append("")
    lines.append("AGGREGATE STATISTICS")
    lines.append("-" * 40)
    lines.append(f"  Total Threads:     {len(results)}")
    lines.append(f"  Successful:        {success_count}")
    lines.append(f"  Failed/Crashed:    {fail_count}")
    lines.append(f"  Total Wall Time:   {total_time_ms:.1f} ms ({total_time_ms/1000:.2f} s)")
    
    if all_latencies:
        lines.append("")
        lines.append("  Per-Thread Latency (sum of all steps):")
        lines.append(f"    Min:    {min(all_latencies):.1f} ms")
        lines.append(f"    Max:    {max(all_latencies):.1f} ms")
        lines.append(f"    Avg:    {sum(all_latencies)/len(all_latencies):.1f} ms")
    
    if step_latencies['step1']:
        lines.append("")
        lines.append("  Per-Step Averages (across all threads):")
        for step, lats in step_latencies.items():
            if lats:
                lines.append(f"    {step}: {sum(lats)/len(lats):.1f} ms avg")
    
    lines.append("")
    lines.append("=" * 90)
    
    return "\n".join(lines)


def format_single_result(result: dict, total_time_ms: float) -> str:
    """Format a single run result."""
    lines = []
    
    lines.append("")
    lines.append("=" * 60)
    lines.append("RESULTS SUMMARY")
    lines.append("=" * 60)
    
    if not result.get('success'):
        lines.append(f"  Status: FAILED")
        lines.append(f"  Error: {result.get('error', 'unknown')}")
    else:
        s1 = result.get('step1', {})
        s2 = result.get('step2', {})
        s3 = result.get('step3', {})
        s4 = result.get('step4', {})
        
        lines.append(f"  Status: OK")
        lines.append(f"  UID: {s1.get('uid', '-')}")
        lines.append(f"  Puzzle ID: {s3.get('puzzle_id', '-')}")
        lines.append("")
        lines.append("  Step Latencies:")
        lines.append(f"    Step 1 (date-picker):      {s1.get('latency_ms', 0):.1f} ms")
        lines.append(f"    Step 2 (postPickerStatus): {s2.get('latency_ms', 0):.1f} ms")
        lines.append(f"    Step 3 (load crossword):   {s3.get('latency_ms', 0):.1f} ms")
        
        s4_iterations = s4.get('iterations', [])
        if s4_iterations:
            s4_latencies = [it.get('latency_ms', 0) for it in s4_iterations]
            lines.append(f"    Step 4 (10 play posts):    {sum(s4_latencies):.1f} ms total, {sum(s4_latencies)/len(s4_latencies):.1f} ms avg")
        
        total_api = sum([
            s1.get('latency_ms', 0),
            s2.get('latency_ms', 0),
            s3.get('latency_ms', 0),
            sum(it.get('latency_ms', 0) for it in s4_iterations)
        ])
        lines.append("")
        lines.append(f"  Total API Time:    {total_api:.1f} ms")
    
    lines.append(f"  Total Wall Time:   {total_time_ms:.1f} ms ({total_time_ms/1000:.2f} s)")
    lines.append("=" * 60)
    
    return "\n".join(lines)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Performance Testing V2 (Hardcoded Puzzle)")
    parser.add_argument("--random-uid", action="store_true", help="generate random uid for each run")
    parser.add_argument("--uid", type=str, default="vansh", help="uid to use")
    parser.add_argument("--uid-pool-size", type=int, default=0, help="size of UID pool")
    parser.add_argument("--parallel", type=int, default=0, help="number of parallel threads")
    parser.add_argument("--rps", type=int, default=0, help="requests per second (wave mode)")
    parser.add_argument("--duration", type=int, default=1, help="duration in seconds (wave mode)")
    parser.add_argument("--title", type=str, default="", help="title for this test run")
    parser.add_argument("--output", type=str, default="", help="output path for CSV results")
    parser.add_argument("--html", action="store_true", help="generate HTML dashboard after saving CSV")
    parser.add_argument("--verbose", "-v", action="store_true", help="print step-by-step progress")
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"V2 Mode: puzzle_id={PUZZLE_ID}, state_len={STATE_LEN}")
        print("")
    
    uid_pool = None
    if args.uid_pool_size > 0:
        uid_pool = [generate_random_uid() for _ in range(args.uid_pool_size)]
        if args.verbose:
            print(f"Generated UID pool of {len(uid_pool)} users")
    
    if args.rps > 0:
        if args.verbose:
            print("=" * 60)
            print(f"Wave Execution Mode (V2)")
            if args.title:
                print(f"Title: {args.title}")
            print("=" * 60 + "\n")
        
        run_data = run_wave_execution(
            rps=args.rps,
            duration=args.duration,
            use_random_uid=args.random_uid,
            uid_pool=uid_pool,
            title=args.title,
            verbose=args.verbose,
        )
        
        print("\n" + "=" * 60)
        print("WAVE EXECUTION SUMMARY")
        print("=" * 60)
        print(f"  Title:            {run_data['title'] or '(none)'}")
        print(f"  Puzzle ID:        {PUZZLE_ID}")
        print(f"  State Length:     {STATE_LEN}")
        print(f"  Config:           {run_data['config']['rps']} rps × {run_data['config']['duration']}s")
        print(f"  Total Wall Time:  {run_data['total_time_ms']:.0f} ms")
        print("")
        print(f"  {'Wave':<6} {'Success':<10} {'Min':<10} {'Max':<10} {'Avg':<10}")
        print("  " + "-" * 46)
        for wave in run_data['waves']:
            success_str = f"{wave['success']}/{wave['threads']}"
            min_val = f"{wave.get('min', 0):.0f}" if wave.get('min') else "-"
            max_val = f"{wave.get('max', 0):.0f}" if wave.get('max') else "-"
            avg_val = f"{wave.get('avg', 0):.0f}" if wave.get('avg') else "-"
            print(f"  {wave['wave_number']:<6} {success_str:<10} {min_val:<10} {max_val:<10} {avg_val:<10}")
        print("=" * 60)
        
        if args.output:
            filepath = save_results_to_csv(run_data, args.output)
            print(f"\nResults saved to: {filepath}")
            
            # generate HTML if requested
            if args.html:
                import subprocess
                html_path = filepath.replace('.csv', '.html')
                subprocess.run(['python3', 'view_results.py', filepath, '-o', html_path], check=True)
                print(f"HTML dashboard: {html_path}")
    
    elif args.parallel > 0:
        if args.verbose:
            print("=" * 60)
            print(f"Running {args.parallel} parallel threads (V2)...")
            print("=" * 60 + "\n")
        
        start_time = time.perf_counter()
        results = run_parallel_threads(
            num_threads=args.parallel,
            use_random_uid=args.random_uid,
            uid_pool=uid_pool,
            verbose=args.verbose,
        )
        total_time_ms = (time.perf_counter() - start_time) * 1000
        
        print(format_results_table(results, total_time_ms))
        
    else:
        if args.verbose:
            print("=" * 60)
            print("Running single thread (V2)...")
            print("=" * 60 + "\n")
        
        start_time = time.perf_counter()
        result = run_single_thread(
            use_random_uid=args.random_uid,
            uid=args.uid,
            uid_pool=uid_pool,
            verbose=args.verbose,
        )
        total_time_ms = (time.perf_counter() - start_time) * 1000
        
        print(format_single_result(result, total_time_ms))

