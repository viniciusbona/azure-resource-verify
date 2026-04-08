#!/usr/bin/env python3
"""
Azure Inventory Script - VERSÃO COMPLETA COM MÉTRICAS AVANÇADAS
Inclui métricas para APIM, Backup Vault (com detalhes de itens), VPN Gateway (tunnel metrics) e Public IPs
"""

import os
import json
import requests
import pandas as pd
import subprocess
import time
import re
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple
from dateutil import parser

from azure.identity import DefaultAzureCredential
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions

# ===== CONFIGURAÇÃO =====
DAYS = 30
OUT_DIR = "output"
REGION = "Multiple"
DELAY_BETWEEN_REQUESTS = 2
ENABLE_METRICS = True
MAX_RETRIES = 3

# ===== SUBSCRIPTIONS PADRÃO =====
DEFAULT_SUBSCRIPTION_IDS = ["d1fe8d89-6fb0-489e-816a-7e9aa0d666aa", "fb61a2b6-5478-488a-a5e6-d123b28d30d9"]

os.makedirs(OUT_DIR, exist_ok=True)

# Variáveis globais para período
START_DATE = None
END_DATE = None

# ===== MAPEAMENTO DE SKU PARA CUSTOS ESTIMADOS =====
SKU_COST_MAP = {
    'microsoft.apimanagement/service': {
        'Developer': 0.0, 'Standard': 0.87, 'Premium': 2.49, 'Basic': 0.42, 'Consumption': 0.0,
    },
    'microsoft.network/virtualnetworkgateways': {
        'VpnGw1': 0.146, 'VpnGw2': 0.292, 'VpnGw3': 0.584, 'VpnGw4': 1.168, 'VpnGw5': 2.336,
        'ErGw1AZ': 0.735, 'ErGw2AZ': 1.47, 'ErGw3AZ': 2.205, 'Basic': 0.073,
    },
    'microsoft.network/bastionhosts': {
        'Basic': 0.19, 'Standard': 0.29, 'Developer': 0.0,
    },
    'microsoft.recoveryservices/vaults': {
        'RS0': 0.0, 'Standard': 0.0,
    },
    'microsoft.network/publicipaddresses': {
        'Basic': 0.0, 'Standard': 0.0242,
    },
}

# ===== FUNÇÕES DE UTILITÁRIO =====
def format_number(value: float) -> str:
    if value >= 1_000_000_000:
        return f"{value/1_000_000_000:.1f}B"
    elif value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value/1_000:.1f}K"
    return f"{value:,.0f}"

def format_bytes(bytes_value: float) -> str:
    """Formata bytes para unidade legível"""
    if bytes_value >= 1_000_000_000_000:
        return f"{bytes_value/1_000_000_000_000:.1f} TB"
    elif bytes_value >= 1_000_000_000:
        return f"{bytes_value/1_000_000_000:.1f} GB"
    elif bytes_value >= 1_000_000:
        return f"{bytes_value/1_000_000:.1f} MB"
    elif bytes_value >= 1_000:
        return f"{bytes_value/1_000:.1f} KB"
    return f"{bytes_value:.0f} B"

# ===== FUNÇÃO PARA OBTER NOMES DAS SUBSCRIPTIONS =====
def get_subscription_names(subscription_ids):
    """Obtém os nomes das subscriptions usando Azure CLI"""
    subscription_names = {}
    try:
        cmd = ["az", "account", "list", "--output", "json"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            accounts = json.loads(result.stdout)
            for account in accounts:
                sub_id = account.get('id', '')
                sub_name = account.get('name', '')
                if sub_id in subscription_ids:
                    subscription_names[sub_id] = sub_name
    except Exception as e:
        print(f"⚠️ Erro ao obter nomes das subscriptions: {e}")
    
    for sub_id in subscription_ids:
        if sub_id not in subscription_names:
            subscription_names[sub_id] = sub_id[:8]
    return subscription_names

# ===== FUNÇÃO PARA VALIDAR SUBSCRIPTION IDS =====
def validate_subscription_ids(subscription_ids):
    """Valida se os subscription IDs são válidos"""
    valid_ids = []
    invalid_ids = []
    for sub_id in subscription_ids:
        try:
            cmd = ["az", "account", "show", "--subscription", sub_id, "--output", "json"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                valid_ids.append(sub_id)
            else:
                invalid_ids.append(sub_id)
        except Exception:
            invalid_ids.append(sub_id)
    
    if invalid_ids:
        print(f"\n⚠️ ATENÇÃO: Subscriptions inválidas ou inacessíveis:")
        for inv_id in invalid_ids:
            print(f"   - {inv_id}")
    return valid_ids, invalid_ids

# ===== FUNÇÃO PARA MÉTRICAS COM RETRY =====
def get_metrics_with_retry(resource_id: str, metric_name: str, aggregation: str, days: int, max_retries: int = MAX_RETRIES) -> Optional[float]:
    """Obtém métricas com retry automático"""
    for attempt in range(max_retries):
        try:
            result = get_metrics_via_cli(resource_id, metric_name, aggregation, days)
            if result is not None:
                return result
            if attempt < max_retries - 1:
                wait_time = DELAY_BETWEEN_REQUESTS * (2 ** attempt)
                time.sleep(wait_time)
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(DELAY_BETWEEN_REQUESTS * (2 ** attempt))
    return None

def get_metrics_via_cli(resource_id: str, metric_name: str, aggregation: str, days: int) -> Optional[float]:
    """Obtém métricas usando Azure CLI"""
    if not ENABLE_METRICS:
        return None
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days)
        cmd = [
            "az", "monitor", "metrics", "list",
            "--resource", resource_id,
            "--metric", metric_name,
            "--aggregation", aggregation,
            "--start-time", start_time.isoformat(),
            "--end-time", end_time.isoformat(),
            "--output", "json"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return None
        data = json.loads(result.stdout)
        total_value = 0
        has_data = False
        for metric in data.get('value', []):
            for timeseries in metric.get('timeseries', []):
                for point in timeseries.get('data', []):
                    value = point.get(aggregation.lower())
                    if value is not None:
                        has_data = True
                        if aggregation.lower() == 'total':
                            total_value += value
                        else:
                            return value
        return total_value if has_data else None
    except Exception:
        return None

# ===== MÉTRICAS AVANÇADAS PARA VPN GATEWAY (COM TUNNEL METRICS) =====
def get_vpn_gateway_metrics_enhanced(resource_id: str) -> str:
    """
    Retorna métricas detalhadas do VPN Gateway com foco em TUNNEL metrics:
    - Tunnel Ingress Bytes (bytes recebidos via túnel)
    - Tunnel Egress Bytes (bytes enviados via túnel)
    - Largura de banda por túnel
    - Status dos túneis
    - Total de tráfego
    """
    summary_parts = []
    gateway_name = resource_id.split('/')[-1]
    resource_group = resource_id.split('/')[4]
    
    # 1. Status do gateway
    try:
        cmd = [
            "az", "network", "vpn-gateway", "show",
            "--ids", resource_id,
            "--query", "provisioningState",
            "--output", "tsv"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            status = result.stdout.strip()
            if status == 'Succeeded':
                summary_parts.append("Status: Ativo")
            else:
                summary_parts.append(f"Status: {status}")
    except:
        pass
    
    # 2. TUNNEL INGRESS BYTES (dados recebidos via túnel)
    tunnel_ingress = get_metrics_with_retry(resource_id, "TunnelIngressBytes", "Total", DAYS)
    if tunnel_ingress is not None and tunnel_ingress > 0:
        ingress_gb = tunnel_ingress / (1024 * 1024 * 1024)
        summary_parts.append(f"Tunnel Ingress: {ingress_gb:.1f} GB")
    else:
        summary_parts.append("Tunnel Ingress: 0 GB")
    
    # 3. TUNNEL EGRESS BYTES (dados enviados via túnel)
    tunnel_egress = get_metrics_with_retry(resource_id, "TunnelEgressBytes", "Total", DAYS)
    if tunnel_egress is not None and tunnel_egress > 0:
        egress_gb = tunnel_egress / (1024 * 1024 * 1024)
        summary_parts.append(f"Tunnel Egress: {egress_gb:.1f} GB")
    else:
        summary_parts.append("Tunnel Egress: 0 GB")
    
    # 4. Largura de banda média do túnel
    avg_bandwidth = get_metrics_with_retry(resource_id, "TunnelAverageBandwidth", "Average", DAYS)
    if avg_bandwidth is not None and avg_bandwidth > 0:
        bandwidth_mbps = avg_bandwidth * 8 / (1024 * 1024)
        if bandwidth_mbps >= 1000:
            summary_parts.append(f"Banda: {bandwidth_mbps/1000:.1f} Gbps")
        else:
            summary_parts.append(f"Banda: {bandwidth_mbps:.1f} Mbps")
    
    # 5. Listar túneis VPN e seus status
    try:
        cmd = [
            "az", "network", "vpn-connection", "list",
            "--resource-group", resource_group,
            "--vpn-gateway", gateway_name,
            "--query", "[].{name:name, connectionStatus:connectionStatus, ingressBytes:ingressBytesTransferred, egressBytes:egressBytesTransferred}",
            "--output", "json"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            connections = json.loads(result.stdout)
            if connections:
                active_tunnels = sum(1 for c in connections if c.get('connectionStatus') == 'Connected')
                total_tunnels = len(connections)
                
                if active_tunnels > 0:
                    summary_parts.append(f"Túneis: {active_tunnels}/{total_tunnels} ativos")
                
                # Mostrar detalhes de cada túnel
                for conn in connections[:3]:  # Limita a 3 túneis
                    conn_name = conn.get('name', '')
                    conn_status = conn.get('connectionStatus', '')
                    ingress = conn.get('ingressBytes', 0) / (1024 * 1024 * 1024)
                    egress = conn.get('egressBytes', 0) / (1024 * 1024 * 1024)
                    
                    if ingress > 0 or egress > 0:
                        summary_parts.append(f"Túnel {conn_name}: {conn_status} (Rx:{ingress:.1f}GB Tx:{egress:.1f}GB)")
                    elif conn_status == 'Connected':
                        summary_parts.append(f"Túnel {conn_name}: {conn_status}")
    
    except Exception as e:
        pass
    
    # 6. Calcular tráfego total
    if tunnel_ingress is not None and tunnel_egress is not None:
        total_traffic_gb = (tunnel_ingress + tunnel_egress) / (1024 * 1024 * 1024)
        if total_traffic_gb > 0:
            summary_parts.append(f"Tráfego Total: {total_traffic_gb:.1f} GB")
    
    # 7. Verificar BGP
    try:
        cmd = [
            "az", "network", "vpn-gateway", "show",
            "--ids", resource_id,
            "--query", "bgpSettings.asn",
            "--output", "tsv"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            bgp_asn = result.stdout.strip()
            if bgp_asn and bgp_asn != 'null':
                summary_parts.append(f"BGP: ASN {bgp_asn}")
    except:
        pass
    
    return "; ".join(summary_parts) if summary_parts else "Sem métricas (gateway pode estar parado)"

# ===== MÉTRICAS AVANÇADAS PARA BACKUP VAULT (COM DETALHES DE ITENS) =====
def get_backup_vault_metrics_enhanced(resource_id: str) -> str:
    """
    Retorna métricas detalhadas do Recovery Services Vault:
    - BACKUP MANAGEMENT TYPE (Azure Backup Server, Azure Backup Agent, Azure Virtual Machine)
    - BACKUP ITEM COUNT por tipo
    - Verificação de itens com último backup > 30 dias
    - Itens candidatos a remoção
    """
    summary_parts = []
    vault_name = resource_id.split('/')[-1]
    resource_group = resource_id.split('/')[4]
    
    # Dicionário para contar itens por tipo
    backup_items_by_type = {
        'Azure Virtual Machine': 0,
        'Azure Backup Agent': 0,
        'Azure Backup Server': 0,
        'SQL in Azure VM': 0,
        'Azure Files': 0,
        'Other': 0
    }
    
    # Lista para armazenar itens com backup antigo
    old_backup_items = []
    total_items = 0
    
    # 1. Obter métrica de Backup Items (Azure Monitor)
    backup_items_metric = get_metrics_with_retry(resource_id, "Backup Items", "Average", DAYS)
    if backup_items_metric is not None and backup_items_metric > 0:
        summary_parts.append(f"Itens Total: {backup_items_metric:.0f}")
    
    # 2. Obter detalhes dos itens via CLI
    try:
        cmd = [
            "az", "backup", "item", "list",
            "--vault-name", vault_name,
            "--resource-group", resource_group,
            "--output", "json"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            items = json.loads(result.stdout)
            total_items = len(items)
            
            for item in items:
                properties = item.get('properties', {})
                workload_type = properties.get('workloadType', '')
                item_name = item.get('name', '')
                
                # Classificar por tipo de workload
                if workload_type == 'VM' or 'AzureVm' in workload_type:
                    backup_items_by_type['Azure Virtual Machine'] += 1
                elif workload_type == 'MAB' or 'AzureBackupServer' in workload_type:
                    backup_items_by_type['Azure Backup Server'] += 1
                elif workload_type == 'FileFolder' or 'AzureBackupAgent' in workload_type:
                    backup_items_by_type['Azure Backup Agent'] += 1
                elif workload_type == 'SQLDataBase':
                    backup_items_by_type['SQL in Azure VM'] += 1
                elif workload_type == 'AzureFileShare':
                    backup_items_by_type['Azure Files'] += 1
                else:
                    backup_items_by_type['Other'] += 1
                
                # 3. Verificar último backup do item
                try:
                    cmd_job = [
                        "az", "backup", "job", "list",
                        "--vault-name", vault_name,
                        "--resource-group", resource_group,
                        "--query", f"[?properties.entityFriendlyName=='{item_name}']",
                        "--output", "json"
                    ]
                    job_result = subprocess.run(cmd_job, capture_output=True, text=True, timeout=30)
                    
                    if job_result.returncode == 0:
                        jobs = json.loads(job_result.stdout)
                        if jobs:
                            # Pegar o job mais recente
                            latest_job = jobs[0]
                            job_status = latest_job.get('properties', {}).get('status', '')
                            end_time = latest_job.get('properties', {}).get('endTime', '')
                            
                            if end_time and job_status == 'Completed':
                                try:
                                    end_date = parser.parse(end_time)
                                    days_ago = (datetime.now(timezone.utc) - end_date).days
                                    
                                    # Verificar se último backup > 30 dias
                                    if days_ago > 30:
                                        old_backup_items.append({
                                            'name': item_name,
                                            'type': workload_type,
                                            'days_ago': days_ago
                                        })
                                except:
                                    pass
                except:
                    pass
    
    except Exception as e:
        print(f"      ⚠️ Erro ao listar itens de backup: {e}")
    
    # 4. Adicionar informações de Backup Management Type
    for backup_type, count in backup_items_by_type.items():
        if count > 0:
            summary_parts.append(f"{backup_type}: {count}")
    
    # 5. Verificar itens com backup antigo
    if old_backup_items:
        summary_parts.append(f"⚠️ {len(old_backup_items)} item(ns) com backup > 30 dias:")
        for item in old_backup_items[:5]:  # Limita a 5 itens na mensagem
            summary_parts.append(f"  - {item['name']} ({item['days_ago']} dias)")
        
        if len(old_backup_items) > 5:
            summary_parts.append(f"  ... e mais {len(old_backup_items) - 5} itens")
    
    # 6. Obter storage utilizado
    backup_storage = get_metrics_with_retry(resource_id, "Backup Storage", "Total", DAYS)
    if backup_storage is not None and backup_storage > 0:
        storage_gb = backup_storage / (1024 * 1024 * 1024)
        summary_parts.append(f"Storage: {storage_gb:.1f} GB")
    
    # 7. Obter política de retenção
    try:
        cmd_policy = [
            "az", "backup", "policy", "list",
            "--vault-name", vault_name,
            "--resource-group", resource_group,
            "--output", "json"
        ]
        policy_result = subprocess.run(cmd_policy, capture_output=True, text=True, timeout=30)
        
        if policy_result.returncode == 0:
            policies = json.loads(policy_result.stdout)
            if policies:
                for policy in policies[:2]:
                    retention = policy.get('properties', {}).get('retentionPolicy', {})
                    daily_retention = retention.get('dailySchedule', {}).get('retentionDuration', {})
                    retention_days = daily_retention.get('count', 0)
                    
                    if retention_days > 0:
                        summary_parts.append(f"Retenção: {retention_days} dias")
                        break
    except:
        pass
    
    # 8. Recomendação final
    if old_backup_items:
        summary_parts.append("🔴 AÇÃO: Remover itens com backup antigo para reduzir custos")
    elif total_items == 0:
        summary_parts.append("🟡 VAULT VAZIO - Considere remover")
    else:
        summary_parts.append("✅ Backup saudável")
    
    return "; ".join(summary_parts) if summary_parts else "Sem métricas de backup"

# ===== MÉTRICAS AVANÇADAS PARA PUBLIC IPs =====
def get_public_ip_metrics_enhanced(resource_id: str, resource_name: str = "") -> str:
    """Retorna métricas detalhadas do Public IP Address"""
    summary_parts = []
    
    # 1. Métricas de pacotes
    packet_count = get_metrics_with_retry(resource_id, "PacketCount", "Total", DAYS)
    if packet_count is not None and packet_count > 0:
        summary_parts.append(f"Pacotes: {format_number(packet_count)}")
    else:
        summary_parts.append("Pacotes: 0")
    
    # 2. Bytes totais
    bytes_total = get_metrics_with_retry(resource_id, "BytesTotal", "Total", DAYS)
    if bytes_total is not None and bytes_total > 0:
        summary_parts.append(f"Tráfego: {format_bytes(bytes_total)}")
    
    # 3. Bytes de entrada (ingress)
    bytes_in = get_metrics_with_retry(resource_id, "BytesIn", "Total", DAYS)
    if bytes_in is not None and bytes_in > 0:
        summary_parts.append(f"Rx: {format_bytes(bytes_in)}")
    
    # 4. Bytes de saída (egress)
    bytes_out = get_metrics_with_retry(resource_id, "BytesOut", "Total", DAYS)
    if bytes_out is not None and bytes_out > 0:
        summary_parts.append(f"Tx: {format_bytes(bytes_out)}")
    
    # 5. Largura de banda média
    avg_bandwidth = get_metrics_with_retry(resource_id, "AverageBandwidth", "Average", DAYS)
    if avg_bandwidth is not None and avg_bandwidth > 0:
        bandwidth_mbps = avg_bandwidth * 8 / (1024 * 1024)
        if bandwidth_mbps >= 1000:
            summary_parts.append(f"Banda: {bandwidth_mbps/1000:.1f} Gbps")
        else:
            summary_parts.append(f"Banda: {bandwidth_mbps:.1f} Mbps")
    
    # 6. Verificar associação
    try:
        cmd = [
            "az", "network", "public-ip", "show",
            "--ids", resource_id,
            "--query", "{ipAddress:ipAddress, associatedResource:ipConfiguration.id}",
            "--output", "json"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            ip_address = data.get('ipAddress', '')
            associated = data.get('associatedResource', '')
            
            if ip_address:
                summary_parts.append(f"IP: {ip_address}")
            
            if associated and associated != 'null':
                if 'virtualMachines' in associated:
                    vm_name = associated.split('/')[-1]
                    summary_parts.append(f"Associado: VM {vm_name}")
                elif 'loadBalancers' in associated:
                    lb_name = associated.split('/')[-1]
                    summary_parts.append(f"Associado: LB {lb_name}")
                elif 'virtualNetworkGateways' in associated:
                    gw_name = associated.split('/')[-1]
                    summary_parts.append(f"Associado: VPN {gw_name}")
                else:
                    summary_parts.append(f"Associado: Sim")
            else:
                summary_parts.append("⚠️ IP ORFÃO - Não associado")
    except:
        pass
    
    # 7. Status de tráfego
    if packet_count is not None and packet_count == 0:
        summary_parts.append("💰 SEM TRÁFEGO - candidato a remoção")
    elif packet_count is not None and packet_count > 0:
        summary_parts.append("✅ COM TRÁFEGO - em uso")
    
    return "; ".join(summary_parts) if summary_parts else "Sem métricas disponíveis"

# ===== MÉTRICAS AVANÇADAS PARA API MANAGEMENT =====
def get_apim_metrics_enhanced(resource_id: str) -> str:
    """Retorna métricas detalhadas do API Management"""
    summary_parts = []
    
    requests = get_metrics_with_retry(resource_id, "Requests", "Total", DAYS)
    if requests is not None and requests > 0:
        summary_parts.append(f"Total Req: {format_number(requests)}")
    
    success_requests = get_metrics_with_retry(resource_id, "SuccessfulRequests", "Total", DAYS)
    if success_requests is not None and success_requests > 0:
        success_rate = (success_requests / requests * 100) if requests and requests > 0 else 0
        summary_parts.append(f"Sucesso: {format_number(success_requests)} ({success_rate:.1f}%)")
    
    failed_requests = get_metrics_with_retry(resource_id, "FailedRequests", "Total", DAYS)
    if failed_requests is not None and failed_requests > 0:
        failed_rate = (failed_requests / requests * 100) if requests and requests > 0 else 0
        summary_parts.append(f"Falhas 4xx: {format_number(failed_requests)} ({failed_rate:.1f}%)")
    
    backend_errors = get_metrics_with_retry(resource_id, "BackendErrors", "Total", DAYS)
    if backend_errors is not None and backend_errors > 0:
        summary_parts.append(f"Erros 5xx: {format_number(backend_errors)}")
    
    duration = get_metrics_with_retry(resource_id, "Duration", "Average", DAYS)
    if duration is not None and duration > 0:
        summary_parts.append(f"Latência: {duration:.0f}ms")
    
    egress_bandwidth = get_metrics_with_retry(resource_id, "Egress", "Total", DAYS)
    if egress_bandwidth is not None and egress_bandwidth > 0:
        egress_mb = egress_bandwidth / (1024 * 1024)
        if egress_mb >= 1024:
            summary_parts.append(f"Download: {egress_mb/1024:.1f} GB")
        else:
            summary_parts.append(f"Download: {egress_mb:.1f} MB")
    
    capacity = get_metrics_with_retry(resource_id, "Capacity", "Average", DAYS)
    if capacity is not None and capacity > 0:
        summary_parts.append(f"Capacidade: {capacity:.0f}%")
    
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

# ===== FUNÇÃO MELHORADA PARA OBTER CUSTO =====
def get_cost_for_resource_enhanced(subscription_id: str, resource_id: str, resource_name: str, 
                                   resource_type: str, sku_name: str, token: str, 
                                   start_date: datetime, end_date: datetime) -> Tuple[float, str]:
    """Versão melhorada para obter custo do recurso com fallback"""
    warning_msg = ""
    
    # Tenta Cost Management API
    url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    body = {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": start_str, "to": end_str},
        "dataset": {
            "granularity": "Daily",
            "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
            "filter": {"dimensions": {"name": "ResourceId", "operator": "In", "values": [resource_id]}}
        }
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    try:
        time.sleep(DELAY_BETWEEN_REQUESTS)
        response = requests.post(url, headers=headers, json=body, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            rows = data.get("properties", {}).get("rows", [])
            if rows and len(rows) > 0 and rows[0][0] is not None:
                cost = float(rows[0][0])
                if cost > 0:
                    return (cost, warning_msg)
    except Exception as e:
        warning_msg = f"Erro na API: {str(e)[:30]}"
    
    # Fallback: estimativa baseada em SKU
    if resource_type in SKU_COST_MAP:
        sku_upper = sku_name.upper()
        for sku_pattern, rate in SKU_COST_MAP[resource_type].items():
            if sku_pattern.upper() in sku_upper:
                hours = DAYS * 24
                estimated_cost = rate * hours
                if estimated_cost > 0:
                    warning_msg = f"⚠️ Custo ESTIMADO"
                    return (estimated_cost, warning_msg)
    
    return (0.0, warning_msg if warning_msg else "SEM DADOS DE CUSTO")

# ===== FUNÇÃO PARA OBTER SKU DINAMICAMENTE =====
def get_resource_sku_dynamic(resource_id: str, resource_type: str) -> str:
    """Obtém SKU do recurso de forma dinâmica"""
    try:
        if resource_type == 'microsoft.compute/virtualmachines':
            cmd = ["az", "vm", "show", "--ids", resource_id, "--query", "hardwareProfile.vmSize", "--output", "tsv"]
        elif resource_type == 'microsoft.compute/disks':
            cmd = ["az", "disk", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        elif resource_type == 'microsoft.storage/storageaccounts':
            cmd = ["az", "storage", "account", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        elif resource_type == 'microsoft.network/publicipaddresses':
            cmd = ["az", "network", "public-ip", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        elif resource_type == 'microsoft.recoveryservices/vaults':
            cmd = ["az", "backup", "vault", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        elif resource_type == 'microsoft.web/serverfarms':
            cmd = ["az", "appservice", "plan", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        elif resource_type == 'microsoft.containerservice/managedclusters':
            cmd = ["az", "aks", "show", "--ids", resource_id, "--query", "sku.tier", "--output", "tsv"]
        elif resource_type == 'microsoft.apimanagement/service':
            cmd = ["az", "apim", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        elif resource_type == 'microsoft.network/virtualnetworkgateways':
            cmd = ["az", "network", "vpn-gateway", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        else:
            cmd = ["az", "resource", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return ""

# ===== FUNÇÃO PARA BUSCAR TODOS OS RECURSOS =====
def get_all_resources(subscription_ids, arg_client):
    """Busca TODOS os recursos via Resource Graph"""
    all_resources = []
    query = """
    resources
    | project 
        id,
        name,
        type,
        resourceGroup,
        subscriptionId,
        location,
        tags
    """
    for sub_id in subscription_ids:
        print(f"\n📡 Buscando recursos na subscription: {sub_id[:8]}...")
        request = QueryRequest(
            query=query,
            subscriptions=[sub_id],
            options=QueryRequestOptions(result_format="objectArray", top=2000)
        )
        try:
            response = arg_client.resources(request)
            if response.data:
                for item in response.data:
                    all_resources.append({
                        'id': item.get('id', ''),
                        'subscriptionId': item.get('subscriptionId', ''),
                        'resourceName': item.get('name', ''),
                        'resourceType': item.get('type', ''),
                        'resourceGroup': item.get('resourceGroup', ''),
                        'region': item.get('location', ''),
                        'tags': json.dumps(item.get('tags', {})) if item.get('tags') else '{}'
                    })
                print(f"  ✅ Encontrados {len(response.data)} recursos")
        except Exception as e:
            print(f"  ❌ Erro: {e}")
    return pd.DataFrame(all_resources)

# ===== FUNÇÃO PARA CLASSIFICAR RECURSOS =====
def classify_resource(resource_type: str) -> Tuple[str, str]:
    """Classifica o recurso por categoria"""
    type_lower = resource_type.lower()
    type_map = {
        'virtualmachines': ("Computação - VM", "Virtual Machine"),
        'disks': ("Armazenamento - Disco", "Managed Disk"),
        'publicipaddresses': ("Rede - IP Público", "Public IP"),
        'networkinterfaces': ("Rede - Interface", "Network Interface"),
        'networksecuritygroups': ("Rede - Segurança", "NSG"),
        'virtualnetworks': ("Rede - Virtual", "VNet"),
        'storageaccounts': ("Armazenamento - Storage", "Storage Account"),
        'recoveryservices': ("Backup - Recovery Vault", "Backup Vault"),
        'restorepointcollections': ("Backup - Pontos Restauração", "Backup Collection"),
        'operationalinsights': ("Monitoramento - Log Analytics", "Log Analytics"),
        'apimanagement': ("Integração - API Management", "API Management"),
        'virtualnetworkgateways': ("Rede - VPN Gateway", "VPN Gateway"),
        'bastionhosts': ("Rede - Bastion", "Bastion"),
        'serverfarms': ("Aplicação - App Service Plan", "App Service Plan"),
        'sites': ("Aplicação - App Service", "App Service"),
        'logic': ("Integração - Logic App", "Logic App"),
        'servicebus': ("Integração - Service Bus", "Service Bus"),
        'documentdb': ("Banco de Dados - Cosmos DB", "Cosmos DB"),
        'containerservice': ("Contêiner - Kubernetes", "AKS"),
        'workflows': ("Integração - Logic App", "Logic App"),
        'connections': ("Integração - Connection", "API Connection"),
    }
    for key, value in type_map.items():
        if key in type_lower:
            return value
    return "Outros", resource_type.split('/')[-1]

# ===== FUNÇÃO PARA IDENTIFICAR DEPENDÊNCIAS =====
def identify_dependencies(df: pd.DataFrame) -> pd.DataFrame:
    """Identifica dependências entre recursos"""
    df['dependencies'] = ""
    df['attached_resources'] = ""
    df['parent_resource'] = ""
    
    for idx, row in df.iterrows():
        resource_type = row['resourceType']
        resource_name = row['resourceName'].lower()
        dependencies = []
        attached = []
        parent = ""
        
        if resource_type == 'microsoft.compute/virtualmachines':
            vm_name = resource_name
            for _, disk in df[df['resourceType'] == 'microsoft.compute/disks'].iterrows():
                if vm_name in disk['resourceName'].lower() or disk['resourceName'].lower() in vm_name:
                    attached.append(f"Disco: {disk['resourceName']}")
                    dependencies.append(f"Disco: {disk['resourceName']}")
            for _, nic in df[df['resourceType'] == 'microsoft.network/networkinterfaces'].iterrows():
                if vm_name in nic['resourceName'].lower():
                    attached.append(f"NIC: {nic['resourceName']}")
                    dependencies.append(f"NIC: {nic['resourceName']}")
            for _, ip in df[df['resourceType'] == 'microsoft.network/publicipaddresses'].iterrows():
                if vm_name in ip['resourceName'].lower():
                    attached.append(f"IP: {ip['resourceName']}")
                    dependencies.append(f"IP: {ip['resourceName']}")
        elif resource_type == 'microsoft.compute/disks':
            disk_name = resource_name
            for _, vm in df[df['resourceType'] == 'microsoft.compute/virtualmachines'].iterrows():
                if disk_name in vm['resourceName'].lower() or vm['resourceName'].lower() in disk_name:
                    parent = vm['resourceName']
                    dependencies.append(f"VM: {vm['resourceName']}")
                    break
        elif resource_type == 'microsoft.network/publicipaddresses':
            ip_name = resource_name
            for _, vm in df[df['resourceType'] == 'microsoft.compute/virtualmachines'].iterrows():
                if ip_name in vm['resourceName'].lower():
                    parent = vm['resourceName']
                    dependencies.append(f"VM: {vm['resourceName']}")
                    break
        
        if dependencies:
            df.at[idx, 'dependencies'] = "; ".join(set(dependencies))
        if attached:
            df.at[idx, 'attached_resources'] = "; ".join(set(attached))
        if parent:
            df.at[idx, 'parent_resource'] = parent
    return df

# ===== FUNÇÃO PRINCIPAL =====
def main():
    global START_DATE, END_DATE
    
    parser = argparse.ArgumentParser(description='Azure Inventory Script - Métricas Avançadas')
    parser.add_argument('--subscription-ids', nargs='+', 
                        help='IDs das subscriptions para análise')
    args = parser.parse_args()
    
    # Obter subscription IDs
    if args.subscription_ids:
        subscription_ids = args.subscription_ids
        print(f"\n📋 Usando subscriptions fornecidas: {len(subscription_ids)}")
    else:
        subscription_ids = DEFAULT_SUBSCRIPTION_IDS
        print(f"\n📋 Usando subscriptions padrão: {', '.join([s[:8] for s in subscription_ids])}")
        print("   Para especificar subscriptions, use: --subscription-ids \"id1\" \"id2\"")
    
    # Validar subscriptions
    valid_ids, invalid_ids = validate_subscription_ids(subscription_ids)
    if not valid_ids:
        print("\n❌ Nenhuma subscription válida encontrada!")
        exit(1)
    
    if invalid_ids:
        print(f"\n⚠️ {len(invalid_ids)} subscription(s) inválida(s) ignorada(s)")
    
    # Obter nomes das subscriptions
    subscription_names = get_subscription_names(valid_ids)
    
    print("\n" + "="*60)
    print("🚀 AZURE INVENTORY SCRIPT - MÉTRICAS AVANÇADAS")
    print("="*60)
    
    print(f"\n📋 Subscriptions a serem analisadas:")
    for sub_id in valid_ids:
        print(f"  • {subscription_names.get(sub_id, sub_id[:8])} ({sub_id})")
    
    # Inicializar clientes
    credential = DefaultAzureCredential()
    arg_client = ResourceGraphClient(credential)
    
    try:
        token = credential.get_token("https://management.azure.com/.default").token
        print("\n✅ Token obtido com sucesso")
    except Exception as e:
        print(f"❌ Erro ao obter token: {e}")
        exit(1)
    
    # Definir período
    END_DATE = datetime.now(timezone.utc)
    START_DATE = END_DATE - timedelta(days=DAYS)
    print(f"Período análise: {START_DATE.date()} → {END_DATE.date()}")
    
    # Buscar recursos
    df = get_all_resources(valid_ids, arg_client)
    if df.empty:
        print("\n❌ Nenhum recurso encontrado!")
        exit(1)
    
    print(f"\n📊 Total de recursos encontrados: {len(df)}")
    
    # Estatísticas por tipo
    print("\n📊 Distribuição por tipo de recurso:")
    type_counts = df['resourceType'].value_counts()
    for rt, count in type_counts.head(20).items():
        type_short = rt.split('/')[-1]
        print(f"  • {type_short:<50} {count:>3} recursos")
    
    # Resetar índice
    df = df.reset_index(drop=True)
    
    # Adicionar colunas
    df['skuName'] = ""
    df['kindName'] = ""
    df['managedBy'] = ""
    df['parent_resource'] = ""
    df['attached_resources'] = ""
    df['dependencies'] = ""
    df['daily_cost_30d'] = 0.0
    df['cost_30d'] = 0.0
    df['cost_warning'] = ""
    df['usage_summary'] = ""
    df['in_use'] = ""
    df['classification'] = ""
    df['purpose'] = ""
    df['removal_impact'] = ""
    df['recommendation'] = ""
    df['orphan_candidate'] = ""
    
    # Identificar dependências
    print("\n🔗 IDENTIFICANDO DEPENDÊNCIAS...")
    df = identify_dependencies(df)
    
    # Obter SKU para tipos específicos
    sku_types = [
        'microsoft.compute/virtualmachines',
        'microsoft.compute/disks',
        'microsoft.storage/storageaccounts',
        'microsoft.network/publicipaddresses',
        'microsoft.recoveryservices/vaults',
        'microsoft.web/serverfarms',
        'microsoft.apimanagement/service',
        'microsoft.network/virtualnetworkgateways'
    ]
    
    print("\n🔄 OBTENDO SKU...")
    for idx, row in df.iterrows():
        if idx % 20 == 0:
            print(f"  Processando SKU: {idx + 1}/{len(df)}")
        if row['resourceType'] in sku_types:
            df.loc[idx, 'skuName'] = get_resource_sku_dynamic(row['id'], row['resourceType'])
    
    # Coletar métricas e custos
    print("\n💰 COLETANDO MÉTRICAS E CUSTOS...")
    
    metric_success = 0
    public_ip_count = 0
    public_ip_with_traffic = 0
    backup_vaults_with_old_items = 0
    
    for idx, row in df.iterrows():
        resource_type = row['resourceType']
        resource_name = row['resourceName']
        type_short = resource_type.split('/')[-1]
        
        if idx % 20 == 0:
            print(f"\nProcessando recurso {idx+1}/{len(df)}: {resource_name[:50]} ({type_short})")
        
        # Coletar métricas específicas por tipo
        summary = ""
        try:
            if resource_type == 'microsoft.apimanagement/service':
                summary = get_apim_metrics_enhanced(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
                print(f"   📊 APIM: {summary[:100]}")
            
            elif resource_type == 'microsoft.recoveryservices/vaults':
                summary = get_backup_vault_metrics_enhanced(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
                if "item(ns) com backup > 30 dias" in summary:
                    backup_vaults_with_old_items += 1
                print(f"   📊 Backup Vault: {summary[:150]}")
            
            elif resource_type == 'microsoft.network/virtualnetworkgateways':
                summary = get_vpn_gateway_metrics_enhanced(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
                print(f"   📊 VPN Gateway: {summary[:120]}")
            
            elif resource_type == 'microsoft.network/publicipaddresses':
                summary = get_public_ip_metrics_enhanced(row['id'], resource_name)
                if summary and "Sem" not in summary:
                    metric_success += 1
                public_ip_count += 1
                if "COM TRÁFEGO" in summary:
                    public_ip_with_traffic += 1
                print(f"   📊 Public IP: {summary[:120]}")
            
            else:
                summary = f"Recurso do tipo {type_short}"
                print(f"   📊 {summary[:100]}")
                
        except Exception as e:
            summary = f"Erro: {str(e)[:50]}"
            print(f"   ⚠️ {summary}")
        
        df.loc[idx, 'usage_summary'] = summary if summary else "Sem métricas disponíveis"
        
        # Coletar custo
        cost, warning = get_cost_for_resource_enhanced(
            row['subscriptionId'], row['id'], row['resourceName'], 
            resource_type, df.loc[idx, 'skuName'], token, START_DATE, END_DATE
        )
        df.loc[idx, 'cost_30d'] = round(cost, 4)
        df.loc[idx, 'daily_cost_30d'] = round(cost / DAYS, 4) if cost > 0 else 0
        df.loc[idx, 'cost_warning'] = warning
        
        if cost > 0:
            print(f"   💰 Custo: R$ {cost:.2f}")
        elif warning:
            print(f"   ⚠️ {warning}")
    
    # Classificação
    print("\n📊 CLASSIFICANDO RECURSOS...")
    
    for idx, row in df.iterrows():
        classification, purpose = classify_resource(row['resourceType'])
        cost = row['cost_30d']
        in_use = "Sim" if cost > 0 else "Não"
        
        # Para Public IPs, usar métrica de tráfego para determinar uso
        if row['resourceType'] == 'microsoft.network/publicipaddresses':
            usage_summary = row.get('usage_summary', '')
            if "COM TRÁFEGO" in usage_summary:
                in_use = "Sim"
            elif "SEM TRÁFEGO" in usage_summary:
                in_use = "Não"
        
        removal_impact = "Baixo"
        if cost > 100:
            removal_impact = "Alto"
        elif cost > 10:
            removal_impact = "Médio"
        
        recommendation = "Manter"
        if cost == 0 and in_use == "Não":
            recommendation = "REVISAR - Sem uso e sem custo"
        elif cost == 0 and in_use == "Sim":
            recommendation = "Manter - Em uso"
        elif cost > 0 and in_use == "Não":
            recommendation = "⚠️ CANDIDATO A REMOÇÃO - Recurso sem uso gerando custo"
        
        orphan_candidate = "Não"
        if row.get('dependencies', '') == "" and (cost == 0 or in_use == "Não"):
            orphan_candidate = "Sim"
        
        df.loc[idx, 'in_use'] = in_use
        df.loc[idx, 'classification'] = classification
        df.loc[idx, 'purpose'] = purpose
        df.loc[idx, 'removal_impact'] = removal_impact
        df.loc[idx, 'recommendation'] = recommendation
        df.loc[idx, 'orphan_candidate'] = orphan_candidate
    
    # Salvar CSV
    output_file = f"{OUT_DIR}/azure_inventory_advanced.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # Estatísticas finais
    print("\n" + "="*60)
    print("📊 ESTATÍSTICAS FINAIS")
    print("="*60)
    
    total_cost = df['cost_30d'].sum()
    print(f"💰 Custo total (30 dias): R$ {total_cost:,.2f}")
    
    print(f"\n📈 RESUMO DA COLETA:")
    print(f"  ✅ Métricas coletadas: {metric_success} recursos")
    print(f"  🌐 Public IPs analisados: {public_ip_count}")
    print(f"     - Com tráfego: {public_ip_with_traffic}")
    print(f"     - Sem tráfego: {public_ip_count - public_ip_with_traffic}")
    print(f"  💾 Backup Vaults com itens antigos (>30 dias): {backup_vaults_with_old_items}")
    
    # Listar Public IPs sem tráfego
    ips_without_traffic = df[
        (df['resourceType'] == 'microsoft.network/publicipaddresses') & 
        (df['usage_summary'].str.contains("SEM TRÁFEGO", na=False))
    ]
    
    if not ips_without_traffic.empty:
        print(f"\n🗑️ PUBLIC IPs SEM TRÁFEGO (candidatos a remoção):")
        for _, ip in ips_without_traffic.iterrows():
            print(f"     - {ip['resourceName']} (custo: R$ {ip['cost_30d']:.2f}) - {ip['usage_summary'][:80]}")
    
    # Listar Backup Vaults com itens antigos
    backup_with_issues = df[
        (df['resourceType'] == 'microsoft.recoveryservices/vaults') & 
        (df['usage_summary'].str.contains("backup > 30 dias", na=False))
    ]
    
    if not backup_with_issues.empty:
        print(f"\n⚠️ BACKUP VAULTS COM ITENS ANTIGOS (>30 dias):")
        for _, vault in backup_with_issues.iterrows():
            print(f"     - {vault['resourceName']}: {vault['usage_summary'][:100]}")
    
    # Resumo por tipo
    print(f"\n📊 Resumo por tipo de recurso:")
    for resource_type in df['resourceType'].unique():
        type_short = resource_type.split('/')[-1]
        count = len(df[df['resourceType'] == resource_type])
        with_metrics = len(df[(df['resourceType'] == resource_type) & (~df['usage_summary'].str.contains("Sem", na=False))])
        print(f"  • {type_short:<50} {count:>3} recursos - {with_metrics:>3} com métricas")
    
    print(f"\n💾 Arquivo salvo: {output_file}")
    print(f"\n✅ Processamento concluído!")

if __name__ == "__main__":
    main()