#!/usr/bin/env python3
"""
Azure Inventory Script - VERSÃO OTIMIZADA COM CLASSIFICAÇÃO PRECISA DE PUBLIC IPs
Inclui métricas para APIM, Backup Vault, VPN Gateway, Public IPs e Cosmos DB.
Otimizado para pular recursos que não geram custo, com logs de duração e timestamp no arquivo de saída.
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
DELAY_BETWEEN_REQUESTS = 2
ENABLE_METRICS = True
MAX_RETRIES = 3

# ===== SUBSCRIPTIONS PADRÃO =====
DEFAULT_SUBSCRIPTION_IDS = ["d1fe8d89-6fb0-489e-816a-7e9aa0d666aa", "fb61a2b6-5478-488a-a5e6-d123b28d30d9"]

os.makedirs(OUT_DIR, exist_ok=True)

# ===== TIMESTAMP PARA O ARQUIVO DE SAÍDA =====
FILE_TIMESTAMP = datetime.now().strftime("%d-%m-%Y-%H%M%S")

# ===== LISTAS DE TIPOS DE RECURSO PARA OTIMIZAÇÃO =====
# Recursos que NUNCA geram custo direto
COST_FREE_RESOURCE_TYPES = [
    'microsoft.network/networksecuritygroups',
    'microsoft.network/virtualnetworks',
    'microsoft.network/routefilters',
    'microsoft.network/routetables',
    'microsoft.network/networkwatchers',
    'microsoft.network/connections',
    'microsoft.network/localnetworkgateways',
    'microsoft.network/privatednszones',
    'microsoft.authorization/roleassignments',
    'microsoft.authorization/roledefinitions',
    'microsoft.insights/actiongroups',
    'microsoft.insights/activitylogalerts',
    'microsoft.insights/metricalerts',
    'microsoft.insights/scheduledqueryrules',
    'microsoft.insights/autoscalesettings',
    'microsoft.insights/diagnosticsettings',
    'microsoft.insights/workbooks',
    'microsoft.insights/workbooktemplates',
    'microsoft.alertsmanagement/smartdetectoralertrules',
    'microsoft.resources/deployments',
    'microsoft.resourcegraph/queries',
    'microsoft.operationsmanagement/solutions',
    'microsoft.managedidentity/userassignedidentities',
    'microsoft.compute/restorepointcollections',
    'microsoft.compute/sshpublickeys',
    'microsoft.compute/virtualmachines/extensions',
    'microsoft.compute/snapshots',
    'microsoft.compute/galleries',
    'microsoft.compute/images',
    'microsoft.web/connections',
    'microsoft.web/serverfarms',
    'microsoft.web/certificates',
    'microsoft.containerregistry/registries',
    'microsoft.containerregistry/registries/webhooks',
    'microsoft.containerregistry/registries/replications',
    'microsoft.dbformariadb/servers',
    'microsoft.dbforpostgresql/servers',
    'microsoft.dbforpostgresql/flexibleservers',
    'microsoft.dbformysql/servers',
    'microsoft.dbformysql/flexibleservers',
    'microsoft.sql/servers',
    'microsoft.sql/servers/databases',
    'microsoft.sql/managedinstances',
    'microsoft.keyvault/vaults',
    'microsoft.cdn/profiles',
    'microsoft.cdn/profiles/endpoints',
    'microsoft.servicebus/namespaces',
    'microsoft.relay/namespaces',
    'microsoft.eventhub/namespaces',
    'microsoft.eventgrid/topics',
    'microsoft.eventgrid/domains',
    'microsoft.eventgrid/systemtopics',
    'microsoft.eventgrid/partnernamespaces',
    'microsoft.eventgrid/partnertopics',
    'microsoft.signalrservice/signalr',
    'microsoft.web/kubeenvironments',
    'microsoft.app/managedenvironments',
    'microsoft.app/containerapps',
    'microsoft.automation/automationaccounts',
    'microsoft.automation/automationaccounts/runbooks',
    'microsoft.automation/automationaccounts/configurations',
    'microsoft.hybridcompute/machines',
    'microsoft.hybridcompute/machines/extensions',
    'microsoft.migrate/assessmentprojects',
    'microsoft.migrate/migrateprojects',
    'microsoft.offazure/serversites',
    'microsoft.visualstudio/account',
    'microsoft.visualstudio/account/project',
    'microsoft.saas/resources',
    'microsoft.databricks/workspaces',
    'microsoft.databricks/accessconnectors',
    'microsoft.devtestlab/schedules',
    'microsoft.portal/dashboards',
    'microsoft.fabric/capacities',
    'microsoft.powerbidedicated/capacities',
    'microsoft.documentdb/databaseaccounts',
]

# Recursos que SEMPRE geram custo (cálculo obrigatório)
ALWAYS_BILLABLE_RESOURCES = [
    'microsoft.compute/virtualmachines',
    'microsoft.compute/disks',
    'microsoft.storage/storageaccounts',
    'microsoft.network/loadbalancers',
    'microsoft.network/bastionhosts',
    'microsoft.network/virtualnetworkgateways',
    'microsoft.recoveryservices/vaults',
    'microsoft.operationalinsights/workspaces',
    'microsoft.apimanagement/service',
    'microsoft.logic/workflows',
    'microsoft.containerservice/managedclusters',
    'microsoft.containerinstance/containergroups',
    'microsoft.cognitiveservices/accounts',
    'microsoft.databricks/workspaces',
    'microsoft.synapse/workspaces',
    'microsoft.machinelearningservices/workspaces',
    'microsoft.azurearcdata/sqlmanagedinstances',
    'microsoft.azurearcdata/postgresinstances',
]

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
    'microsoft.compute/virtualmachines': {
        'Standard_B1s': 0.018, 'Standard_B2s': 0.036, 'Standard_D2s_v3': 0.098, 'Standard_D4s_v3': 0.196,
        'Standard_E2s_v3': 0.124, 'Standard_E4s_v3': 0.248, 'Standard_E8s_v3': 0.496,
        'Standard_F2s_v2': 0.084, 'Standard_F4s_v2': 0.168, 'Standard_F8s_v2': 0.336,
    },
}

# ===== TABELA DE PREÇOS DO COSMOS DB (RU/hora em USD) =====
COSMOS_RU_PRICES = {
    'Manual': {
        100: 0.008, 200: 0.016, 400: 0.032, 1000: 0.08,
        2000: 0.16, 3000: 0.24, 4000: 0.32, 5000: 0.40,
        10000: 0.80, 15000: 1.20, 20000: 1.60
    },
    'Autoscale': {
        1000: 0.08, 2000: 0.16, 4000: 0.32, 5000: 0.40,
        10000: 0.80, 20000: 1.60
    }
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
    if bytes_value >= 1_000_000_000_000:
        return f"{bytes_value/1_000_000_000_000:.1f} TB"
    elif bytes_value >= 1_000_000_000:
        return f"{bytes_value/1_000_000_000:.1f} GB"
    elif bytes_value >= 1_000_000:
        return f"{bytes_value/1_000_000:.1f} MB"
    elif bytes_value >= 1_000:
        return f"{bytes_value/1_000:.1f} KB"
    return f"{bytes_value:.0f} B"

# ===== FUNÇÃO MELHORADA PARA CLASSIFICAR SE RECURSO É FATURÁVEL =====
def is_billable_resource(resource_type: str, resource_id: str = "", token: str = "") -> Tuple[bool, bool, str]:
    """
    Determina se um recurso deve ser analisado para custos.
    Retorna: (deve_analisar_custo, eh_sempre_faturavel, motivo)
    
    Para Public IPs, a análise considera:
    - SKU (Basic vs Standard)
    - Status de associação (se está vinculado a algum recurso)
    """
    resource_type_lower = resource_type.lower()
    
    # ===== TRATAMENTO ESPECIAL PARA PUBLIC IPs =====
    if resource_type_lower == 'microsoft.network/publicipaddresses':
        sku_name = ""
        is_associated = False
        
        # Obter SKU do IP
        try:
            cmd = [
                "az", "network", "public-ip", "show",
                "--ids", resource_id,
                "--query", "{sku:sku.name, associatedResource:ipConfiguration.id}",
                "--output", "json"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                sku_name = data.get('sku', '')
                associated = data.get('associatedResource', '')
                is_associated = associated is not None and associated != 'null'
        except:
            pass
        
        # IP Standard (Estático) SEMPRE gera custo
        if 'Standard' in sku_name:
            return (True, True, f"IP Standard (estático) - sempre gera custo")
        
        # IP Basic (Dinâmico) com associação
        if 'Basic' in sku_name and is_associated:
            return (True, False, f"IP Basic associado - custo depende do estado da VM")
        
        # IP Basic (Dinâmico) sem associação - NÃO gera custo
        if 'Basic' in sku_name and not is_associated:
            return (False, False, f"IP Basic órfão - não associado, não gera custo")
        
        # SKU desconhecido, assume análise
        return (True, False, f"IP com SKU desconhecido - analisar")
    
    # ===== RECURSOS QUE SEMPRE GERAM CUSTO =====
    for billable in ALWAYS_BILLABLE_RESOURCES:
        if billable in resource_type_lower:
            return (True, True, f"Recurso sempre faturável")
    
    # ===== RECURSOS QUE NUNCA GERAM CUSTO =====
    for free in COST_FREE_RESOURCE_TYPES:
        if free in resource_type_lower:
            return (False, False, f"Recurso gratuito/não faturável")
    
    # ===== RECURSOS QUE PODEM GERAR CUSTO (análise necessária) =====
    return (True, False, f"Recurso potencialmente faturável")

# ===== FUNÇÕES DE AUTENTICAÇÃO E VALIDAÇÃO =====
def get_subscription_names(subscription_ids):
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

def validate_subscription_ids(subscription_ids):
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

# ===== FUNÇÕES DE MÉTRICAS ESPECÍFICAS =====
def get_public_ip_metrics_enhanced(resource_id: str, resource_name: str = "") -> str:
    summary_parts = []
    
    # 1. Obter SKU e associação para informação
    try:
        cmd = [
            "az", "network", "public-ip", "show",
            "--ids", resource_id,
            "--query", "{sku:sku.name, ipAddress:ipAddress, associatedResource:ipConfiguration.id, provisioningState:provisioningState}",
            "--output", "json"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            sku_name = data.get('sku', '')
            ip_address = data.get('ipAddress', '')
            associated = data.get('associatedResource', '')
            
            if ip_address:
                summary_parts.append(f"IP: {ip_address}")
            
            if sku_name:
                summary_parts.append(f"SKU: {sku_name}")
            
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
                elif 'natGateways' in associated:
                    nat_name = associated.split('/')[-1]
                    summary_parts.append(f"Associado: NAT {nat_name}")
                else:
                    summary_parts.append(f"Associado: Sim")
            else:
                summary_parts.append("⚠️ IP ORFÃO - Não associado")
    except:
        pass
    
    # 2. Métricas de pacotes
    packet_count = get_metrics_with_retry(resource_id, "PacketCount", "Total", DAYS)
    if packet_count is not None and packet_count > 0:
        summary_parts.append(f"Pacotes: {format_number(packet_count)}")
    else:
        summary_parts.append("Pacotes: 0")
    
    # 3. Bytes totais
    bytes_total = get_metrics_with_retry(resource_id, "BytesTotal", "Total", DAYS)
    if bytes_total is not None and bytes_total > 0:
        summary_parts.append(f"Tráfego: {format_bytes(bytes_total)}")
    
    # 4. Bytes de entrada (ingress)
    bytes_in = get_metrics_with_retry(resource_id, "BytesIn", "Total", DAYS)
    if bytes_in is not None and bytes_in > 0:
        summary_parts.append(f"Rx: {format_bytes(bytes_in)}")
    
    # 5. Bytes de saída (egress)
    bytes_out = get_metrics_with_retry(resource_id, "BytesOut", "Total", DAYS)
    if bytes_out is not None and bytes_out > 0:
        summary_parts.append(f"Tx: {format_bytes(bytes_out)}")
    
    # 6. Largura de banda média
    avg_bandwidth = get_metrics_with_retry(resource_id, "AverageBandwidth", "Average", DAYS)
    if avg_bandwidth is not None and avg_bandwidth > 0:
        bandwidth_mbps = avg_bandwidth * 8 / (1024 * 1024)
        if bandwidth_mbps >= 1000:
            summary_parts.append(f"Banda: {bandwidth_mbps/1000:.1f} Gbps")
        else:
            summary_parts.append(f"Banda: {bandwidth_mbps:.1f} Mbps")
    
    # 7. Status de tráfego e recomendação
    if packet_count is not None and packet_count == 0:
        summary_parts.append("💰 SEM TRÁFEGO - candidato a remoção")
    elif packet_count is not None and packet_count > 0:
        summary_parts.append("✅ COM TRÁFEGO - em uso")
    
    return "; ".join(summary_parts) if summary_parts else "Sem métricas disponíveis"

def get_apim_metrics_enhanced(resource_id: str) -> str:
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

def get_backup_vault_metrics_enhanced(resource_id: str) -> str:
    summary_parts = []
    vault_name = resource_id.split('/')[-1]
    resource_group = resource_id.split('/')[4]
    
    backup_items = get_metrics_with_retry(resource_id, "Backup Items", "Average", DAYS)
    if backup_items is not None and backup_items > 0:
        summary_parts.append(f"Itens: {backup_items:.0f}")
    
    backup_storage = get_metrics_with_retry(resource_id, "Backup Storage", "Total", DAYS)
    if backup_storage is not None and backup_storage > 0:
        storage_gb = backup_storage / (1024 * 1024 * 1024)
        summary_parts.append(f"Storage: {storage_gb:.1f} GB")
    
    try:
        cmd = [
            "az", "backup", "item", "list",
            "--vault-name", vault_name,
            "--resource-group", resource_group,
            "--output", "json"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            items = json.loads(result.stdout)
            if items:
                vm_count = sum(1 for i in items if i.get('properties', {}).get('workloadType') == 'VM')
                sql_count = sum(1 for i in items if i.get('properties', {}).get('workloadType') == 'SQLDataBase')
                
                if vm_count > 0:
                    summary_parts.append(f"VMs: {vm_count}")
                if sql_count > 0:
                    summary_parts.append(f"SQL: {sql_count}")
                
                last_backup_dates = []
                for item in items[:5]:
                    item_name = item.get('name', '')
                    if item_name:
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
                                latest_job = jobs[0]
                                job_status = latest_job.get('properties', {}).get('status', '')
                                end_time = latest_job.get('properties', {}).get('endTime', '')
                                
                                if end_time and job_status == 'Completed':
                                    try:
                                        end_date = parser.parse(end_time)
                                        days_ago = (datetime.now(timezone.utc) - end_date).days
                                        last_backup_dates.append(days_ago)
                                    except:
                                        pass
                
                if last_backup_dates:
                    last_backup_days = min(last_backup_dates)
                    if last_backup_days <= 1:
                        summary_parts.append("Backup: hoje")
                    elif last_backup_days <= 7:
                        summary_parts.append(f"Backup: há {last_backup_days} dias")
                    else:
                        summary_parts.append(f"Backup: há {last_backup_days} dias ⚠️")
    
    except Exception:
        pass
    
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
    
    return "; ".join(summary_parts) if summary_parts else "Sem métricas de backup"

def get_vpn_gateway_metrics_enhanced(resource_id: str) -> str:
    summary_parts = []
    gateway_name = resource_id.split('/')[-1]
    resource_group = resource_id.split('/')[4]
    
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
    
    tunnel_ingress = get_metrics_with_retry(resource_id, "TunnelIngressBytes", "Total", DAYS)
    if tunnel_ingress is not None and tunnel_ingress > 0:
        ingress_gb = tunnel_ingress / (1024 * 1024 * 1024)
        summary_parts.append(f"Tunnel Ingress: {ingress_gb:.1f} GB")
    else:
        summary_parts.append("Tunnel Ingress: 0 GB")
    
    tunnel_egress = get_metrics_with_retry(resource_id, "TunnelEgressBytes", "Total", DAYS)
    if tunnel_egress is not None and tunnel_egress > 0:
        egress_gb = tunnel_egress / (1024 * 1024 * 1024)
        summary_parts.append(f"Tunnel Egress: {egress_gb:.1f} GB")
    else:
        summary_parts.append("Tunnel Egress: 0 GB")
    
    avg_bandwidth = get_metrics_with_retry(resource_id, "TunnelAverageBandwidth", "Average", DAYS)
    if avg_bandwidth is not None and avg_bandwidth > 0:
        bandwidth_mbps = avg_bandwidth * 8 / (1024 * 1024)
        summary_parts.append(f"Banda: {bandwidth_mbps:.1f} Mbps")
    
    try:
        cmd = [
            "az", "network", "vpn-connection", "list",
            "--resource-group", resource_group,
            "--vpn-gateway", gateway_name,
            "--query", "[].{name:name, connectionStatus:connectionStatus}",
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
    except:
        pass
    
    return "; ".join(summary_parts) if summary_parts else "Sem métricas (gateway pode estar parado)"

def get_cosmos_db_metrics_enhanced(resource_id: str) -> str:
    summary_parts = []
    database_name = resource_id.split('/')[-1]
    resource_group = resource_id.split('/')[4]
    
    ru_configured = 0
    is_autoscale = False
    
    try:
        cmd = [
            "az", "cosmosdb", "sql", "database", "list",
            "--resource-group", resource_group,
            "--account-name", database_name,
            "--query", "[0].options",
            "--output", "json"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            if data:
                options = data[0] if isinstance(data, list) else data
                if options.get('throughput'):
                    ru_configured = int(options.get('throughput'))
                    summary_parts.append(f"RU config: {format_number(ru_configured)} (Manual)")
                elif options.get('autoscaleSettings'):
                    ru_configured = int(options.get('autoscaleSettings', {}).get('maxThroughput', 0))
                    is_autoscale = True
                    summary_parts.append(f"RU config: {format_number(ru_configured)} (Autoscale)")
    except:
        pass
    
    if ru_configured == 0:
        try:
            cmd = [
                "az", "cosmosdb", "sql", "container", "list",
                "--resource-group", resource_group,
                "--account-name", database_name,
                "--database-name", database_name,
                "--query", "[0].options.throughput",
                "--output", "tsv"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0 and result.stdout.strip():
                ru_configured = int(float(result.stdout.strip()))
                summary_parts.append(f"RU config: {format_number(ru_configured)} (Manual)")
        except:
            pass
    
    total_requests = get_metrics_with_retry(resource_id, "TotalRequests", "Total", DAYS)
    if total_requests is not None and total_requests > 0:
        summary_parts.append(f"Requisições: {format_number(total_requests)}")
    
    total_ru = get_metrics_with_retry(resource_id, "TotalRequestUnits", "Total", DAYS)
    if total_ru is not None and total_ru > 0:
        summary_parts.append(f"RU Total: {format_number(total_ru)}")
        avg_ru_per_second = total_ru / (DAYS * 24 * 3600)
        summary_parts.append(f"RU médio/s: {avg_ru_per_second:.1f}")
    
    max_ru_percent = get_metrics_with_retry(resource_id, "NormalizedRUConsumption", "Maximum", DAYS)
    if max_ru_percent is not None and max_ru_percent > 0:
        summary_parts.append(f"Max RU Usage: {max_ru_percent:.1f}%")
        
        if ru_configured > 0:
            peak_ru_used = (max_ru_percent / 100) * ru_configured
            summary_parts.append(f"Pico RU usado: {peak_ru_used:.0f}")
    
    avg_ru_percent = get_metrics_with_retry(resource_id, "NormalizedRUConsumption", "Average", DAYS)
    if avg_ru_percent is not None and avg_ru_percent > 0:
        summary_parts.append(f"Média RU Usage: {avg_ru_percent:.1f}%")
    
    throttled_requests = get_metrics_with_retry(resource_id, "ThrottledRequests", "Total", DAYS)
    if throttled_requests is not None and throttled_requests > 0 and total_requests and total_requests > 0:
        throttled_percent = (throttled_requests / total_requests * 100)
        summary_parts.append(f"⚠️ Requests throttled: {format_number(throttled_requests)} ({throttled_percent:.1f}%)")
    
    if ru_configured > 0 and max_ru_percent is not None and max_ru_percent > 0:
        recommended_ru = None
        peak_ru_used = (max_ru_percent / 100) * ru_configured
        recommended_ru = int(peak_ru_used * 1.1)
        
        if recommended_ru < 100:
            recommended_ru = 100
        elif recommended_ru < 200:
            recommended_ru = 200
        elif recommended_ru < 400:
            recommended_ru = 400
        elif recommended_ru < 1000:
            recommended_ru = 1000
        elif recommended_ru < 2000:
            recommended_ru = 2000
        elif recommended_ru < 4000:
            recommended_ru = 4000
        elif recommended_ru < 5000:
            recommended_ru = 5000
        elif recommended_ru < 10000:
            recommended_ru = 10000
        elif recommended_ru < 20000:
            recommended_ru = 20000
        else:
            recommended_ru = ((recommended_ru + 5000) // 5000) * 5000
        
        if recommended_ru < ru_configured:
            ru_reduction = ru_configured - recommended_ru
            reduction_percent = (ru_reduction / ru_configured) * 100
            
            hourly_rate = 0.00014
            estimated_savings_usd = ru_reduction * hourly_rate * DAYS * 24
            estimated_savings_brl = estimated_savings_usd * 5.5
            
            summary_parts.append(f"🎯 Recomendação: REDUZIR RU de {format_number(ru_configured)} para {format_number(recommended_ru)}")
            summary_parts.append(f"   Redução: {ru_reduction} RU (-{reduction_percent:.1f}%)")
            summary_parts.append(f"   Economia estimada: ~R$ {estimated_savings_brl:.2f}/mês")
            
            if max_ru_percent < 50:
                summary_parts.append(f"   ✅ RECURSO SUPERDIMENSIONADO - uso máximo de apenas {max_ru_percent:.1f}%")
                
        elif recommended_ru > ru_configured:
            ru_increase = recommended_ru - ru_configured
            increase_percent = (ru_increase / ru_configured) * 100
            summary_parts.append(f"⚠️ Recomendação: AUMENTAR RU de {format_number(ru_configured)} para {format_number(recommended_ru)}")
            summary_parts.append(f"   Aumento: {ru_increase} RU (+{increase_percent:.1f}%)")
            
            if throttled_requests and throttled_requests > 0:
                summary_parts.append(f"   ⚠️ Há {format_number(throttled_requests)} requisições limitadas por falta de RU")
        
        elif abs(recommended_ru - ru_configured) / ru_configured < 0.1:
            summary_parts.append(f"✅ CONFIGURAÇÃO OTIMIZADA - RU adequado para a carga")
    
    data_usage = get_metrics_with_retry(resource_id, "DataUsage", "Average", DAYS)
    if data_usage is not None and data_usage > 0:
        storage_gb = data_usage / (1024 * 1024 * 1024)
        summary_parts.append(f"Storage: {storage_gb:.1f} GB")
    
    return "; ".join(summary_parts) if summary_parts else "Sem métricas disponíveis"

# ===== FUNÇÃO MELHORADA PARA OBTER CUSTO =====
def get_cost_for_resource_enhanced(subscription_id: str, resource_id: str, resource_name: str, 
                                   resource_type: str, sku_name: str, token: str, 
                                   start_date: datetime, end_date: datetime, should_analyze: bool) -> Tuple[float, str]:
    """Versão melhorada que pode pular análise para recursos não-faturáveis"""
    
    if not should_analyze:
        return (0.0, "Recurso gratuito/não faturável - análise de custo ignorada")
    
    warning_msg = ""
    
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
                    warning_msg = f"⚠️ Custo ESTIMADO (SKU: {sku_name})"
                    return (estimated_cost, warning_msg)
    
    # Fallback específico para Cosmos DB
    if resource_type == 'microsoft.documentdb/databaseaccounts':
        try:
            ru_configured = 4000
            is_autoscale = False
            price_table = COSMOS_RU_PRICES['Autoscale' if is_autoscale else 'Manual']
            closest_ru = min(price_table.keys(), key=lambda x: abs(x - ru_configured))
            price_per_hour = price_table[closest_ru]
            hours = DAYS * 24
            estimated_cost = price_per_hour * hours
            if estimated_cost > 0:
                warning_msg = f"⚠️ Custo ESTIMADO (Cosmos DB: {ru_configured} RU/s)"
                return (estimated_cost, warning_msg)
        except:
            pass
    
    return (0.0, warning_msg if warning_msg else "SEM DADOS DE CUSTO")

# ===== FUNÇÃO PARA OBTER SKU DINAMICAMENTE =====
def get_resource_sku_dynamic(resource_id: str, resource_type: str) -> str:
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
        elif resource_type == 'microsoft.documentdb/databaseaccounts':
            cmd = ["az", "cosmosdb", "show", "--ids", resource_id, "--query", "capabilities[0].name", "--output", "tsv"]
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
    
    # ===== LOG DE INÍCIO DA EXECUÇÃO =====
    start_time = datetime.now()
    print("\n" + "="*70)
    print("🚀 AZURE INVENTORY SCRIPT - MÉTRICAS AVANÇADAS (VERSÃO OTIMIZADA)")
    print("="*70)
    print(f"📅 Início da execução: {start_time.strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*70)
    
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
    print(f"📅 Período análise: {START_DATE.date()} → {END_DATE.date()}")
    
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
        'microsoft.network/virtualnetworkgateways',
        'microsoft.documentdb/databaseaccounts'
    ]
    
    print("\n🔄 OBTENDO SKU...")
    sku_processed = 0
    for idx, row in df.iterrows():
        if row['resourceType'] in sku_types:
            df.loc[idx, 'skuName'] = get_resource_sku_dynamic(row['id'], row['resourceType'])
            sku_processed += 1
            if sku_processed % 50 == 0:
                print(f"  Processados {sku_processed} SKUs...")
    print(f"  ✅ SKU obtidos para {sku_processed} recursos")
    
    # Coletar métricas e custos
    print("\n💰 COLETANDO MÉTRICAS E CUSTOS...")
    print("   (Recursos gratuitos/não faturáveis serão ignorados para otimização)")
    
    metric_success = 0
    public_ip_count = 0
    public_ip_with_traffic = 0
    public_ip_standard = 0
    public_ip_basic_orphan = 0
    cosmos_optimization_count = 0
    skipped_resources = 0
    analyzed_resources = 0
    
    for idx, row in df.iterrows():
        resource_type = row['resourceType']
        resource_name = row['resourceName']
        type_short = resource_type.split('/')[-1]
        
        # Determinar se o recurso deve ser analisado para custo (usando função melhorada)
        should_analyze_cost, is_always_billable, reason = is_billable_resource(
            resource_type, row['id'], token
        )
        
        if not should_analyze_cost:
            # Pula completamente a análise de custo para recursos não-faturáveis
            df.loc[idx, 'usage_summary'] = f"Recurso gratuito/não faturável - {reason}"
            df.loc[idx, 'cost_warning'] = "Recurso gratuito - análise ignorada"
            df.loc[idx, 'cost_30d'] = 0.0
            df.loc[idx, 'daily_cost_30d'] = 0.0
            skipped_resources += 1
            continue
        
        analyzed_resources += 1
        
        if analyzed_resources % 20 == 0:
            print(f"\nProcessando recurso {analyzed_resources}/{len(df)} (total: {len(df)}): {resource_name[:50]} ({type_short})")
        
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
                print(f"   📊 Backup Vault: {summary[:100]}")
            
            elif resource_type == 'microsoft.network/virtualnetworkgateways':
                summary = get_vpn_gateway_metrics_enhanced(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
                print(f"   📊 VPN Gateway: {summary[:100]}")
            
            elif resource_type == 'microsoft.network/publicipaddresses':
                summary = get_public_ip_metrics_enhanced(row['id'], resource_name)
                if summary and "Sem" not in summary:
                    metric_success += 1
                public_ip_count += 1
                
                # Estatísticas específicas de Public IP
                if "COM TRÁFEGO" in summary:
                    public_ip_with_traffic += 1
                if "SKU: Standard" in summary:
                    public_ip_standard += 1
                if "IP ORFÃO" in summary and "SKU: Basic" in summary:
                    public_ip_basic_orphan += 1
                    
                print(f"   📊 Public IP: {summary[:120]}")
            
            elif resource_type == 'microsoft.documentdb/databaseaccounts':
                summary = get_cosmos_db_metrics_enhanced(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
                if "REDUZIR RU" in summary or "AUMENTAR RU" in summary:
                    cosmos_optimization_count += 1
                print(f"   📊 Cosmos DB: {summary[:150]}")
            
            else:
                # Para outros recursos faturáveis, apenas registrar
                summary = f"Recurso faturável do tipo {type_short} - {reason}"
                if is_always_billable:
                    print(f"   📊 {summary}")
                else:
                    # Para recursos que podem ser faturáveis, mostrar apenas se houver algo relevante
                    pass
        
        except Exception as e:
            summary = f"Erro na coleta: {str(e)[:50]}"
            print(f"   ⚠️ {summary}")
        
        df.loc[idx, 'usage_summary'] = summary if summary else "Sem métricas disponíveis"
        
        # Coletar custo apenas para recursos que devem ser analisados
        cost, warning = get_cost_for_resource_enhanced(
            row['subscriptionId'], row['id'], row['resourceName'], 
            resource_type, df.loc[idx, 'skuName'], token, START_DATE, END_DATE, should_analyze_cost
        )
        df.loc[idx, 'cost_30d'] = round(cost, 4)
        df.loc[idx, 'daily_cost_30d'] = round(cost / DAYS, 4) if cost > 0 else 0
        df.loc[idx, 'cost_warning'] = warning
        
        # Log apenas para recursos com custo positivo ou warnings relevantes
        if cost > 0:
            print(f"   💰 Custo: R$ {cost:.2f} ({warning if warning else 'API Cost Management'})")
        elif warning and "Recurso gratuito" not in warning and "análise ignorada" not in warning:
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
        if cost == 0 and in_use == "Não" and "Recurso gratuito" not in str(row.get('usage_summary', '')):
            recommendation = "REVISAR - Sem uso e sem custo"
        elif cost == 0 and in_use == "Sim":
            recommendation = "Manter - Em uso"
        elif cost > 0 and in_use == "Não":
            recommendation = "⚠️ CANDIDATO A REMOÇÃO - Recurso sem uso gerando custo"
        elif row['resourceType'] == 'microsoft.documentdb/databaseaccounts':
            usage_summary = row.get('usage_summary', '')
            if "REDUZIR RU" in usage_summary:
                recommendation = "🎯 OTIMIZAR - Reduzir RU para economizar"
            elif "AUMENTAR RU" in usage_summary:
                recommendation = "⚠️ OTIMIZAR - Aumentar RU para evitar throttling"
            elif "OTIMIZADA" in usage_summary:
                recommendation = "✅ Configuração otimizada"
        
        orphan_candidate = "Não"
        if row.get('dependencies', '') == "" and (cost == 0 or in_use == "Não"):
            if "Recurso gratuito" not in str(row.get('usage_summary', '')):
                orphan_candidate = "Sim"
        
        df.loc[idx, 'in_use'] = in_use
        df.loc[idx, 'classification'] = classification
        df.loc[idx, 'purpose'] = purpose
        df.loc[idx, 'removal_impact'] = removal_impact
        df.loc[idx, 'recommendation'] = recommendation
        df.loc[idx, 'orphan_candidate'] = orphan_candidate
    
    # Salvar CSV com timestamp no nome do arquivo
    output_file = f"{OUT_DIR}/azure_inventory_advanced_{FILE_TIMESTAMP}.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # ===== LOG DE FINALIZAÇÃO COM DURAÇÃO =====
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*70)
    print("📊 ESTATÍSTICAS FINAIS")
    print("="*70)
    print(f"📅 Início:  {start_time.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"📅 Término: {end_time.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"⏱️  Duração total: {duration.total_seconds():.2f} segundos ({duration.total_seconds()/60:.2f} minutos)")
    print("="*70)
    
    total_cost = df['cost_30d'].sum()
    print(f"💰 Custo total (30 dias): R$ {total_cost:,.2f}")
    
    print(f"\n📈 RESUMO DA COLETA:")
    print(f"  ✅ Recursos analisados: {analyzed_resources}/{len(df)}")
    print(f"  ⏭️  Recursos ignorados (gratuitos): {skipped_resources}")
    print(f"  ✅ Métricas detalhadas coletadas: {metric_success} recursos")
    
    print(f"\n🌐 ESTATÍSTICAS DE PUBLIC IPs:")
    print(f"  📊 Total de Public IPs analisados: {public_ip_count}")
    print(f"  ✅ Com tráfego: {public_ip_with_traffic}")
    print(f"  ❌ Sem tráfego: {public_ip_count - public_ip_with_traffic}")
    print(f"  🔷 IPs Standard (sempre faturam): {public_ip_standard}")
    print(f"  🟢 IPs Basic órfãos (não faturam): {public_ip_basic_orphan}")
    
    print(f"\n🗄️ COSMOS DB:")
    print(f"  🎯 Com oportunidades de otimização: {cosmos_optimization_count}")
    
    # Listar Public IPs sem tráfego
    ips_without_traffic = df[
        (df['resourceType'] == 'microsoft.network/publicipaddresses') & 
        (df['usage_summary'].str.contains("SEM TRÁFEGO", na=False))
    ]
    
    if not ips_without_traffic.empty:
        print(f"\n🗑️ PUBLIC IPs SEM TRÁFEGO (candidatos a remoção):")
        for _, ip in ips_without_traffic.head(10).iterrows():
            sku_info = "Standard" if "SKU: Standard" in str(ip['usage_summary']) else "Basic"
            print(f"     - {ip['resourceName']} ({sku_info}) - custo: R$ {ip['cost_30d']:.2f}")
        if len(ips_without_traffic) > 10:
            print(f"     ... e mais {len(ips_without_traffic) - 10} IPs")
    
    # Listar Cosmos DB com recomendações
    cosmos_to_optimize = df[
        (df['resourceType'] == 'microsoft.documentdb/databaseaccounts') & 
        ((df['usage_summary'].str.contains("REDUZIR RU", na=False)) |
         (df['usage_summary'].str.contains("AUMENTAR RU", na=False)))
    ]
    
    if not cosmos_to_optimize.empty:
        print(f"\n🎯 COSMOS DB COM OPORTUNIDADES DE OTIMIZAÇÃO:")
        for _, cosmos in cosmos_to_optimize.iterrows():
            print(f"     - {cosmos['resourceName']}: {cosmos['usage_summary'][:100]}")
    
    # Resumo por tipo
    print(f"\n📊 Resumo por tipo de recurso (apenas faturáveis):")
    for resource_type in df['resourceType'].unique():
        type_short = resource_type.split('/')[-1]
        count = len(df[df['resourceType'] == resource_type])
        total_type_cost = df[df['resourceType'] == resource_type]['cost_30d'].sum()
        if total_type_cost > 0 or count > 0:
            print(f"  • {type_short:<50} {count:>3} recursos - R$ {total_type_cost:>10,.2f}")
    
    print(f"\n💾 Arquivo salvo: {output_file}")
    print(f"\n✅ Processamento concluído!")

if __name__ == "__main__":
    main()