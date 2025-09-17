#----------------------------------------------------------------------------
# Author: Niloy Saha
# Email: niloysaha.ns@gmail.com
# version ='1.0.0'
# ---------------------------------------------------------------------------
"""
Prometheus exporter which exports slice throughput, packet loss, latency and jitter KPIs.
For use with the 5G-MONARCH project and Open5GS.
Latency and Jitter calculations depend on an external Blackbox Exporter.
"""
import os
import logging
import time
import requests
import prometheus_client as prom
import argparse

from dotenv import load_dotenv

load_dotenv()
MONARCH_THANOS_URL = os.getenv("MONARCH_THANOS_URL")
DEFAULT_UPDATE_PERIOD = 1
UPDATE_PERIOD = int(os.environ.get('UPDATE_PERIOD', DEFAULT_UPDATE_PERIOD))
EXPORTER_PORT = 9000
TIME_RANGE = os.getenv("TIME_RANGE", "5s")


# Prometheus variables
SLICE_THROUGHPUT = prom.Gauge('slice_throughput', 'throughput per slice (bits/sec)', ['snssai', 'seid', 'direction'])
SLICE_PACKET_LOSS = prom.Gauge('slice_packet_loss_ratio', 'packet loss ratio per slice', ['snssai', 'direction'])
SLICE_LATENCY = prom.Gauge('slice_latency_seconds', 'average latency per slice', ['snssai'])
SLICE_JITTER = prom.Gauge('slice_jitter_seconds', 'jitter per slice', ['snssai'])

# get rid of bloat
prom.REGISTRY.unregister(prom.PROCESS_COLLECTOR)
prom.REGISTRY.unregister(prom.PLATFORM_COLLECTOR)
prom.REGISTRY.unregister(prom.GC_COLLECTOR)

def query_prometheus(params, url):
    """
    Query Prometheus using requests and return value.
    params: The parameters for the Prometheus query.
    url: The URL of the Prometheus server.
    Returns: The result of the Prometheus query.
    """
    try:
        r = requests.get(url + '/api/v1/query', params)
        data = r.json()

        results = data["data"]["result"]
        return results
        
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to query Prometheus: {e}")
    except (KeyError, IndexError, ValueError) as e:
        log.error(f"Failed to parse Prometheus response: {e}")
        log.warning("No data available!")
    return [] # Return empty list on failure

def get_slice_throughput_per_seid_and_direction(snssai, direction):
    """
    Queries both the SMF and UPF to get the throughput per SEID and direction.
    Returns a dictionary of the form {seid: value (bits/sec)}
    """
    time_range = TIME_RANGE
    throughput_per_seid = {}  # {seid: value (bits/sec)}

    direction_mapping = {
        "uplink": "outdatavolumen3upf",
        "downlink": "indatavolumen3upf"
    }

    if direction not in direction_mapping:
        log.error("Invalid direction")
        return

    query = f'sum by (seid) (rate(fivegs_ep_n3_gtp_{direction_mapping[direction]}_seid[{time_range}]) * on (seid) group_right sum(fivegs_smffunction_sm_seid_session{{snssai="{snssai}"}}) by (seid, snssai)) * 8'
    log.debug(query)
    params = {'query': query}
    results = query_prometheus(params, MONARCH_THANOS_URL)

    if results:
        for result in results:
            seid = result["metric"]["seid"]
            value = float(result["value"][1])
            throughput_per_seid[seid] = value

    return throughput_per_seid

def get_slice_packet_loss(snssai, direction):
    """
    计算每个切片的丢包率。
    返回一个字典 {snssai: value (ratio)}
    """
    time_range = TIME_RANGE
    packet_loss_per_slice = {}

    direction_mapping_packets = {
        "uplink": "ul_packets",
        "downlink": "dl_packets"
    }
    direction_mapping_dropped = {
        "uplink": "ul_packets_dropped",
        "downlink": "dl_packets_dropped"
    }

    if direction not in direction_mapping_packets:
        log.error("Invalid direction for packet loss")
        return packet_loss_per_slice

    # PromQL查询: (丢包速率 / 总包速率)，如果总包速率为0则结果为0
    # 我们通过 smf function 关联，确保只计算属于该切片的流量
    # 注意: 此处假设存在一个 upf_smf_association 指标用于关联UPF实例和SNSSAI
    query = (
        f'(sum(rate(fivegs_ep_n3_gtp_{direction_mapping_dropped[direction]}_total[{time_range}])) by (instance) '
        f'and on(instance) '
        f'sum(upf_smf_association{{snssai="{snssai}"}}) by (instance))'
        f'/ on(instance) '
        f'(sum(rate(fivegs_ep_n3_gtp_{direction_mapping_packets[direction]}_total[{time_range}])) by (instance) '
        f'and on(instance) '
        f'sum(upf_smf_association{{snssai="{snssai}"}}) by (instance))'
    )
    
    log.debug(query)
    params = {'query': query}
    results = query_prometheus(params, MONARCH_THANOS_URL)

    if results:
        # 假设一个切片只有一个UPF，直接取第一个结果
        value = float(results[0]["value"][1])
        packet_loss_per_slice[snssai] = value

    return packet_loss_per_slice

def get_slice_latency_and_jitter(snssai):
    """
    从 Blackbox Exporter 获取延迟和抖动。
    返回两个字典: {snssai: latency}, {snssai: jitter}
    """
    time_range = TIME_RANGE
    latency_per_slice = {}
    jitter_per_slice = {}

    # 假设 Blackbox Exporter 暴露的指标带有 'slice_id' 标签
    # 我们将 snssai 中的 '-' 替换为 '_' 来匹配标签格式
    slice_label_value = snssai.replace('-', '_')

    # 计算平均延迟
    latency_query = f'avg_over_time(probe_duration_seconds{{slice_id="{slice_label_value}"}}[{time_range}])'
    log.debug(latency_query)
    latency_params = {'query': latency_query}
    latency_results = query_prometheus(latency_params, MONARCH_THANOS_URL)

    if latency_results:
        value = float(latency_results[0]["value"][1])
        latency_per_slice[snssai] = value

    # 计算抖动 (延迟的标准差)
    jitter_query = f'stddev_over_time(probe_duration_seconds{{slice_id="{slice_label_value}"}}[{time_range}])'
    log.debug(jitter_query)
    jitter_params = {'query': jitter_query}
    jitter_results = query_prometheus(jitter_params, MONARCH_THANOS_URL)

    if jitter_results:
        value = float(jitter_results[0]["value"][1])
        jitter_per_slice[snssai] = value
        
    return latency_per_slice, jitter_per_slice
   
def get_active_snssais():
    """
    Return a list of active SNSSAIs from the SMF.
    """
    time_range = TIME_RANGE
    query = f'sum by (snssai) (rate(fivegs_smffunction_sm_seid_session[{time_range}]))'
    log.debug(query)
    params = {'query': query}
    results = query_prometheus(params, MONARCH_THANOS_URL)
    active_snssais = [result["metric"]["snssai"] for result in results]
    return active_snssais

def main():
    log.info("Starting Prometheus server on port {}".format(EXPORTER_PORT))

    if not MONARCH_THANOS_URL:
        log.error("MONARCH_THANOS_URL is not set")
        return 

    log.info(f"Monarch Thanos URL: {MONARCH_THANOS_URL}")
    log.info(f"Time range: {TIME_RANGE}")
    log.info(f"Update period: {UPDATE_PERIOD}")
    prom.start_http_server(EXPORTER_PORT)

    while True:
        try:
            run_kpi_computation()
        except Exception as e:
            log.error(f"Failing to run KPI computation: {e}")
        time.sleep(UPDATE_PERIOD)

def export_to_prometheus(snssai, seid, direction, value):
    value_mbits = round(value / 10 ** 6, 6)
    log.info(f"SNSSAI={snssai} | SEID={seid} | DIR={direction:8s} | RATE (Mbps)={value_mbits}")
    SLICE_THROUGHPUT.labels(snssai=snssai, seid=seid, direction=direction).set(value)

def export_packet_loss_to_prometheus(snssai, direction, value):
    log.info(f"SNSSAI={snssai} | DIR={direction:8s} | PKT_LOSS_RATIO={value:.6f}")
    SLICE_PACKET_LOSS.labels(snssai=snssai, direction=direction).set(value)

def export_latency_jitter_to_prometheus(snssai, latency, jitter):
    log.info(f"SNSSAI={snssai} | LATENCY (s)={latency:.6f} | JITTER (s)={jitter:.6f}")
    SLICE_LATENCY.labels(snssai=snssai).set(latency)
    SLICE_JITTER.labels(snssai=snssai).set(jitter)

def run_kpi_computation():
    directions = ["uplink", "downlink"]
    active_snssais = get_active_snssais()
    if not active_snssais:
        log.warning("No active SNSSAIs found")
        return
    
    log.debug(f"Active SNSSAIs: {active_snssais}")
    for snssai in active_snssais:
        # 计算和导出吞吐量
        for direction in directions:
            throughput_per_seid = get_slice_throughput_per_seid_and_direction(snssai, direction)
            for seid, value in throughput_per_seid.items():
                export_to_prometheus(snssai, seid, direction, value)
        
        # 计算和导出丢包率
        for direction in directions:
            packet_loss_per_slice = get_slice_packet_loss(snssai, direction)
            for slice_id, value in packet_loss_per_slice.items():
                export_packet_loss_to_prometheus(slice_id, direction, value)

        # 计算和导出延迟和抖动
        latency_per_slice, jitter_per_slice = get_slice_latency_and_jitter(snssai)
        if snssai in latency_per_slice and snssai in jitter_per_slice:
            export_latency_jitter_to_prometheus(snssai, latency_per_slice[snssai], jitter_per_slice[snssai])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='KPI calculator.')
    parser.add_argument('--log', default='info', help='Log verbosity level. Default is "info". Options are "debug", "info", "warning", "error", "critical".')

    args = parser.parse_args()

    # Convert log level from string to logging level
    log_level = getattr(logging, args.log.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError(f'Invalid log level: {args.log}')
    
    # setup logger for console output
    log = logging.getLogger(__name__)
    log.setLevel(log_level)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    log.addHandler(console_handler)
        
    main()