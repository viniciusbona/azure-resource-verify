#!/usr/bin/env python3
"""
Azure Inventory Script - VERSÃO CORRIGIDA COM API DE DETALHES DE CUSTO
Utiliza a API generateCostDetailsReport para obter custos precisos como no portal.
Inclui métricas para APIM, Backup Vault, VPN Gateway, Public IPs e Cosmos DB.
"""

import os
import json
import requests
import pandas as pd
import subprocess
import time
import re
import argparse
import csv
import io
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple
from urllib.parse import urlparse
import tempfile

from azure.identity import DefaultAzureCredential
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions

# ===== CONFIGURAÇÃO =====
DAYS = 30
OUT_DIR = "output"
DELAY_BETWEEN_REQUESTS = 1
ENABLE_METRICS = True
MAX_RETRIES = 5
POLLING_INTERVAL = 5  # segundos entre verificações de status do relatório
MAX_POLLING_ATTEMPTS = 60  # máximo de 5 minutos de espera

os.makedirs(OUT_DIR, exist_ok=True)

# ===== TIMESTAMP PARA O ARQUIVO DE SAÍDA =====
FILE_TIMESTAMP = datetime.now().strftime("%d-%m-%Y-%H%M%S")

# ===== LISTAS DE TIPOS DE RECURSO PARA OTIMIZAÇÃO =====
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
    'microsoft.keyvault/vaults',
    'microsoft.servicebus/namespaces',
    'microsoft.relay/namespaces',
    'microsoft.eventhub/namespaces',
    'microsoft.eventgrid/topics',
    'microsoft.automation/automationaccounts',
    'microsoft.automation/automationaccounts/runbooks',
    'microsoft.migrate/assessmentprojects',
    'microsoft.migrate/migrateprojects',
    'microsoft.offazure/serversites',
    'microsoft.visualstudio/account',
    'microsoft.saas/resources',
    'microsoft.devtestlab/schedules',
    'microsoft.portal/dashboards',
    'microsoft.fabric/capacities',
    'microsoft.powerbidedicated/capacities',
]

# ===== SUBSCRIPTIONS PADRÃO =====
DEFAULT_SUBSCRIPTION_IDS = ["d1fe8d89-6fb0-489e-816a-7e9aa0d666aa", "fb61a2b6-5478-488a-a5e6-d123b28d30d9"]


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


def get_subscription_names(subscription_ids):
    """Obtém os nomes das subscriptions"""
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
    """Valida subscriptions via Azure CLI"""
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


# ===== FUNÇÃO PRINCIPAL PARA OBTER CUSTOS VIA API DE DETALHES =====
def get_cost_details_report(subscription_id: str, token: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
    """
    Gera um relatório de custos detalhado usando a API generateCostDetailsReport
    Retorna um DataFrame com os custos por recurso
    """
    print(f"\n💰 Solicitando relatório de custos para subscription {subscription_id[:8]}...")
    
    url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/generateCostDetailsReport?api-version=2024-08-01"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    body = {
        "metric": "ActualCost",
        "timePeriod": {
            "start": start_date.strftime("%Y-%m-%d"),
            "end": end_date.strftime("%Y-%m-%d")
        }
    }
    
    try:
        # Iniciar geração do relatório
        response = requests.post(url, headers=headers, json=body, timeout=60)
        
        if response.status_code == 202:
            operation_location = response.headers.get('Location', '')
            if not operation_location:
                print("   ❌ Não foi possível obter o location da operação")
                return None
            
            print(f"   ✅ Relatório solicitado com sucesso. Aguardando processamento...")
            
            # Aguardar conclusão
            for attempt in range(MAX_POLLING_ATTEMPTS):
                time.sleep(POLLING_INTERVAL)
                
                status_response = requests.get(operation_location, headers=headers, timeout=30)
                
                if status_response.status_code == 200:
                    status_data = status_response.json()
                    status = status_data.get('status', '')
                    
                    print(f"   ⏳ Status: {status} (tentativa {attempt + 1}/{MAX_POLLING_ATTEMPTS})")
                    
                    if status == 'Completed':
                        # Obter link do CSV
                        blob_link = status_data.get('manifest', {}).get('blobLink')
                        if blob_link:
                            print(f"   ✅ Relatório gerado! Baixando arquivo...")
                            
                            # Baixar o CSV
                            csv_response = requests.get(blob_link, timeout=120)
                            if csv_response.status_code == 200:
                                # O conteúdo pode ser ZIP ou CSV direto
                                content = csv_response.content
                                
                                # Verificar se é ZIP
                                if content[:2] == b'PK':
                                    with zipfile.ZipFile(io.BytesIO(content)) as z:
                                        csv_filename = [f for f in z.namelist() if f.endswith('.csv')][0]
                                        with z.open(csv_filename) as csv_file:
                                            df = pd.read_csv(csv_file)
                                else:
                                    # CSV direto
                                    df = pd.read_csv(io.BytesIO(content))
                                
                                print(f"   ✅ Relatório carregado com {len(df)} linhas")
                                return df
                            else:
                                print(f"   ❌ Erro ao baixar CSV: {csv_response.status_code}")
                                return None
                    elif status == 'Failed':
                        error_msg = status_data.get('error', {}).get('message', 'Erro desconhecido')
                        print(f"   ❌ Geração do relatório falhou: {error_msg}")
                        return None
                else:
                    print(f"   ⚠️ Erro ao verificar status: {status_response.status_code}")
            
            print(f"   ❌ Timeout aguardando geração do relatório")
            return None
            
        else:
            print(f"   ❌ Erro ao solicitar relatório: {response.status_code}")
            print(f"   Resposta: {response.text[:200]}")
            return None
            
    except Exception as e:
        print(f"   ❌ Exceção ao obter relatório de custos: {e}")
        return None


def get_all_costs_for_subscriptions(subscription_ids: List[str], token: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Obtém custos para todas as subscriptions e consolida em um único DataFrame
    """
    all_costs = []
    
    for sub_id in subscription_ids:
        df_cost = get_cost_details_report(sub_id, token, start_date, end_date)
        if df_cost is not None and not df_cost.empty:
            all_costs.append(df_cost)
        else:
            print(f"   ⚠️ Nenhum dado de custo obtido para subscription {sub_id[:8]}")
    
    if not all_costs:
        return pd.DataFrame()
    
    # Consolidar todos os DataFrames
    combined_df = pd.concat(all_costs, ignore_index=True)
    
    # Filtrar colunas relevantes e renomear
    # As colunas podem variar, mas geralmente incluem:
    # - ResourceId
    # - CostInBillingCurrency
    # - Currency
    # - Date
    
    # Mapeamento de colunas comuns
    column_mapping = {
        'ResourceId': 'ResourceId',
        'resourceId': 'ResourceId',
        'CostInBillingCurrency': 'CostInBillingCurrency',
        'costInBillingCurrency': 'CostInBillingCurrency',
        'Currency': 'Currency',
        'currency': 'Currency',
        'Date': 'Date',
        'date': 'Date'
    }
    
    # Renomear colunas para padronização
    for old_name, new_name in column_mapping.items():
        if old_name in combined_df.columns:
            combined_df = combined_df.rename(columns={old_name: new_name})
    
    # Agregar custo por ResourceId
    if 'ResourceId' in combined_df.columns and 'CostInBillingCurrency' in combined_df.columns:
        # Converter para numérico
        combined_df['CostInBillingCurrency'] = pd.to_numeric(combined_df['CostInBillingCurrency'], errors='coerce')
        
        # Agrupar por ResourceId
        cost_summary = combined_df.groupby('ResourceId')['CostInBillingCurrency'].sum().reset_index()
        cost_summary.columns = ['ResourceId', 'cost_30d']
        
        print(f"\n💰 Total de recursos com custo identificados: {len(cost_summary)}")
        print(f"💰 Custo total (30 dias): R$ {cost_summary['cost_30d'].sum():,.2f}")
        
        return cost_summary
    else:
        print(f"   ⚠️ Colunas esperadas não encontradas no relatório de custos")
        print(f"   Colunas disponíveis: {list(combined_df.columns)}")
        return pd.DataFrame()


# ===== FUNÇÕES DE MÉTRICAS (mantidas do script original) =====
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


def get_public_ip_metrics_enhanced(resource_id: str, resource_name: str = "") -> str:
    summary_parts = []
    
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
                summary_parts.append(f"Associado: Sim")
            else:
                summary_parts.append("⚠️ IP ORFÃO - Não associado")
    except:
        pass
    
    packet_count = get_metrics_with_retry(resource_id, "PacketCount", "Total", DAYS)
    if packet_count is not None and packet_count > 0:
        summary_parts.append(f"Pacotes: {format_number(packet_count)}")
    else:
        summary_parts.append("Pacotes: 0")
    
    bytes_total = get_metrics_with_retry(resource_id, "BytesTotal", "Total", DAYS)
    if bytes_total is not None and bytes_total > 0:
        summary_parts.append(f"Tráfego: {format_bytes(bytes_total)}")
    
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
    if success_requests is not None and success_requests > 0 and requests:
        success_rate = (success_requests / requests * 100)
        summary_parts.append(f"Sucesso: {format_number(success_requests)} ({success_rate:.1f}%)")
    
    failed_requests = get_metrics_with_retry(resource_id, "FailedRequests", "Total", DAYS)
    if failed_requests is not None and failed_requests > 0 and requests:
        failed_rate = (failed_requests / requests * 100)
        summary_parts.append(f"Falhas 4xx: {format_number(failed_requests)} ({failed_rate:.1f}%)")
    
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
    except:
        pass
    
    return "; ".join(summary_parts) if summary_parts else "Sem métricas de backup"

def get_vpn_gateway_metrics_enhanced(resource_id: str) -> str:
    summary_parts = []
    
    tunnel_ingress = get_metrics_with_retry(resource_id, "TunnelIngressBytes", "Total", DAYS)
    if tunnel_ingress is not None:
        ingress_gb = tunnel_ingress / (1024 * 1024 * 1024)
        summary_parts.append(f"Tunnel Ingress: {ingress_gb:.1f} GB")
    else:
        summary_parts.append("Tunnel Ingress: 0 GB")
    
    tunnel_egress = get_metrics_with_retry(resource_id, "TunnelEgressBytes", "Total", DAYS)
    if tunnel_egress is not None:
        egress_gb = tunnel_egress / (1024 * 1024 * 1024)
        summary_parts.append(f"Tunnel Egress: {egress_gb:.1f} GB")
    else:
        summary_parts.append("Tunnel Egress: 0 GB")
    
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

def get_cosmos_db_metrics_enhanced(resource_id: str) -> str:
    summary_parts = []
    
    total_requests = get_metrics_with_retry(resource_id, "TotalRequests", "Total", DAYS)
    if total_requests is not None and total_requests > 0:
        summary_parts.append(f"Requisições: {format_number(total_requests)}")
    
    total_ru = get_metrics_with_retry(resource_id, "TotalRequestUnits", "Total", DAYS)
    if total_ru is not None and total_ru > 0:
        summary_parts.append(f"RU Total: {format_number(total_ru)}")
    
    max_ru_percent = get_metrics_with_retry(resource_id, "NormalizedRUConsumption", "Maximum", DAYS)
    if max_ru_percent is not None and max_ru_percent > 0:
        summary_parts.append(f"Max RU Usage: {max_ru_percent:.1f}%")
    
    data_usage = get_metrics_with_retry(resource_id, "DataUsage", "Average", DAYS)
    if data_usage is not None and data_usage > 0:
        storage_gb = data_usage / (1024 * 1024 * 1024)
        summary_parts.append(f"Storage: {storage_gb:.1f} GB")
    
    return "; ".join(summary_parts) if summary_parts else "Sem métricas disponíveis"


# ===== FUNÇÃO PARA OBTER SKU =====
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
        elif resource_type == 'microsoft.apimanagement/service':
            cmd = ["az", "apim", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        elif resource_type == 'microsoft.network/virtualnetworkgateways':
            cmd = ["az", "network", "vpn-gateway", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        else:
            return ""
        
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
        'operationalinsights': ("Monitoramento - Log Analytics", "Log Analytics"),
        'apimanagement': ("Integração - API Management", "API Management"),
        'virtualnetworkgateways': ("Rede - VPN Gateway", "VPN Gateway"),
        'bastionhosts': ("Rede - Bastion", "Bastion"),
        'sites': ("Aplicação - App Service", "App Service"),
        'logic': ("Integração - Logic App", "Logic App"),
        'documentdb': ("Banco de Dados - Cosmos DB", "Cosmos DB"),
        'containerservice': ("Contêiner - Kubernetes", "AKS"),
        'workflows': ("Integração - Logic App", "Logic App"),
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
            for _, nic in df[df['resourceType'] == 'microsoft.network/networkinterfaces'].iterrows():
                if vm_name in nic['resourceName'].lower():
                    attached.append(f"NIC: {nic['resourceName']}")
            for _, ip in df[df['resourceType'] == 'microsoft.network/publicipaddresses'].iterrows():
                if vm_name in ip['resourceName'].lower():
                    attached.append(f"IP: {ip['resourceName']}")
        elif resource_type == 'microsoft.compute/disks':
            disk_name = resource_name
            for _, vm in df[df['resourceType'] == 'microsoft.compute/virtualmachines'].iterrows():
                if disk_name in vm['resourceName'].lower() or vm['resourceName'].lower() in disk_name:
                    parent = vm['resourceName']
                    break
        elif resource_type == 'microsoft.network/publicipaddresses':
            ip_name = resource_name
            for _, vm in df[df['resourceType'] == 'microsoft.compute/virtualmachines'].iterrows():
                if ip_name in vm['resourceName'].lower():
                    parent = vm['resourceName']
                    break
        
        if dependencies:
            df.at[idx, 'dependencies'] = "; ".join(set(dependencies))
        if attached:
            df.at[idx, 'attached_resources'] = "; ".join(set(attached))
        if parent:
            df.at[idx, 'parent_resource'] = parent
    return df


def is_billable_resource(resource_type: str) -> Tuple[bool, bool, str]:
    """Determina se um recurso deve ser analisado"""
    resource_type_lower = resource_type.lower()
    
    for free in COST_FREE_RESOURCE_TYPES:
        if free in resource_type_lower:
            return (False, False, "Recurso gratuito/não faturável")
    
    return (True, False, "Recurso potencialmente faturável")


# ===== FUNÇÃO PRINCIPAL =====
def main():
    global START_DATE, END_DATE
    
    start_time = datetime.now()
    print("\n" + "="*70)
    print("🚀 AZURE INVENTORY SCRIPT - VERSÃO COM CUSTOS PRECISOS")
    print("   (Utiliza API de Detalhes de Custo - mesmo método do portal)")
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
    
    # Validar subscriptions
    valid_ids, invalid_ids = validate_subscription_ids(subscription_ids)
    if not valid_ids:
        print("\n❌ Nenhuma subscription válida encontrada!")
        exit(1)
    
    if invalid_ids:
        print(f"\n⚠️ {len(invalid_ids)} subscription(s) inválida(s) ignorada(s)")
    
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
    
    # ===== OBTER CUSTOS VIA API DE DETALHES =====
    print("\n" + "="*70)
    print("💰 OBTENDO CUSTOS VIA API DE DETALHES")
    print("="*70)
    
    cost_df = get_all_costs_for_subscriptions(valid_ids, token, START_DATE, END_DATE)
    
    if cost_df.empty:
        print("\n❌ Não foi possível obter dados de custo via API!")
        print("   Verifique se a subscription tem custos no período e se a API está acessível.")
        exit(1)
    
    # ===== BUSCAR RECURSOS =====
    print("\n" + "="*70)
    print("📡 BUSCANDO RECURSOS")
    print("="*70)
    
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
    
    df = df.reset_index(drop=True)
    
    # ===== JUNTAR CUSTOS COM RECURSOS =====
    print("\n" + "="*70)
    print("🔗 JUNTANDO CUSTOS COM RECURSOS")
    print("="*70)
    
    # Adicionar coluna ResourceId para merge
    df['ResourceId'] = df['id']
    
    # Fazer merge dos custos
    df = df.merge(cost_df[['ResourceId', 'cost_30d']], on='ResourceId', how='left')
    df['cost_30d'] = df['cost_30d'].fillna(0)
    
    print(f"✅ Recursos com custo identificado: {len(df[df['cost_30d'] > 0])}")
    print(f"💰 Custo total (30 dias): R$ {df['cost_30d'].sum():,.2f}")
    
    # Adicionar colunas
    df['daily_cost_30d'] = df['cost_30d'] / DAYS
    df['skuName'] = ""
    df['kindName'] = ""
    df['managedBy'] = ""
    df['parent_resource'] = ""
    df['attached_resources'] = ""
    df['dependencies'] = ""
    df['cost_warning'] = "Custo via API de Detalhes"
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
        'microsoft.apimanagement/service',
        'microsoft.network/virtualnetworkgateways',
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
    
    # Coletar métricas
    print("\n📊 COLETANDO MÉTRICAS...")
    metric_success = 0
    public_ip_count = 0
    public_ip_with_traffic = 0
    analyzed_resources = 0
    
    for idx, row in df.iterrows():
        resource_type = row['resourceType']
        resource_name = row['resourceName']
        type_short = resource_type.split('/')[-1]
        
        should_analyze, _, _ = is_billable_resource(resource_type)
        
        if not should_analyze:
            df.loc[idx, 'usage_summary'] = "Recurso gratuito/não faturável"
            df.loc[idx, 'cost_warning'] = "Recurso gratuito - análise ignorada"
            continue
        
        analyzed_resources += 1
        
        if analyzed_resources % 20 == 0:
            print(f"\nProcessando recurso {analyzed_resources}/{len(df)}: {resource_name[:50]}")
        
        summary = ""
        try:
            if resource_type == 'microsoft.apimanagement/service':
                summary = get_apim_metrics_enhanced(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            
            elif resource_type == 'microsoft.recoveryservices/vaults':
                summary = get_backup_vault_metrics_enhanced(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            
            elif resource_type == 'microsoft.network/virtualnetworkgateways':
                summary = get_vpn_gateway_metrics_enhanced(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            
            elif resource_type == 'microsoft.network/publicipaddresses':
                summary = get_public_ip_metrics_enhanced(row['id'], resource_name)
                if summary and "Sem" not in summary:
                    metric_success += 1
                public_ip_count += 1
                if "COM TRÁFEGO" in summary:
                    public_ip_with_traffic += 1
            
            elif resource_type == 'microsoft.documentdb/databaseaccounts':
                summary = get_cosmos_db_metrics_enhanced(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            
            else:
                summary = f"Recurso faturável do tipo {type_short}"
        
        except Exception as e:
            summary = f"Erro na coleta: {str(e)[:50]}"
        
        df.loc[idx, 'usage_summary'] = summary if summary else "Sem métricas disponíveis"
    
    # Classificação
    print("\n📊 CLASSIFICANDO RECURSOS...")
    
    for idx, row in df.iterrows():
        classification, purpose = classify_resource(row['resourceType'])
        cost = row['cost_30d']
        in_use = "Sim" if cost > 0 else "Não"
        
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
        
        orphan_candidate = "Não"
        if row.get('dependencies', '') == "" and cost == 0:
            if "Recurso gratuito" not in str(row.get('usage_summary', '')):
                orphan_candidate = "Sim"
        
        df.loc[idx, 'in_use'] = in_use
        df.loc[idx, 'classification'] = classification
        df.loc[idx, 'purpose'] = purpose
        df.loc[idx, 'removal_impact'] = removal_impact
        df.loc[idx, 'recommendation'] = recommendation
        df.loc[idx, 'orphan_candidate'] = orphan_candidate
    
    # Renomear colunas para compatibilidade
    df = df.rename(columns={
        'id': 'id',
        'subscriptionId': 'subscriptionId',
        'resourceName': 'resourceName',
        'resourceType': 'resourceType',
        'resourceGroup': 'resourceGroup',
        'region': 'region',
        'tags': 'tags'
    })
    
    # Salvar CSV
    output_file = f"{OUT_DIR}/azure_inventory_advanced_{FILE_TIMESTAMP}.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # ===== LOG DE FINALIZAÇÃO =====
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*70)
    print("📊 ESTATÍSTICAS FINAIS")
    print("="*70)
    print(f"📅 Início:  {start_time.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"📅 Término: {end_time.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"⏱️  Duração total: {duration.total_seconds():.2f} segundos")
    print("="*70)
    
    total_cost = df['cost_30d'].sum()
    print(f"💰 Custo total (30 dias): R$ {total_cost:,.2f}")
    
    print(f"\n📈 RESUMO DA COLETA:")
    print(f"  ✅ Recursos analisados: {analyzed_resources}/{len(df)}")
    print(f"  ✅ Métricas detalhadas coletadas: {metric_success} recursos")
    print(f"  💰 Custo via API de Detalhes: valores precisos como no portal")
    
    print(f"\n🌐 ESTATÍSTICAS DE PUBLIC IPs:")
    print(f"  📊 Total de Public IPs: {public_ip_count}")
    print(f"  ✅ Com tráfego: {public_ip_with_traffic}")
    print(f"  ❌ Sem tráfego: {public_ip_count - public_ip_with_traffic}")
    
    # Listar top 10 recursos por custo
    print(f"\n💰 TOP 10 RECURSOS POR CUSTO (30 dias):")
    top_costs = df.nlargest(10, 'cost_30d')[['resourceName', 'resourceType', 'cost_30d']]
    for _, row in top_costs.iterrows():
        type_short = row['resourceType'].split('/')[-1][:30]
        print(f"  • {row['resourceName'][:40]:<40} - {type_short:<30} - R$ {row['cost_30d']:>10,.2f}")
    
    print(f"\n💾 Arquivo salvo: {output_file}")
    print(f"\n✅ Processamento concluído!")

if __name__ == "__main__":
    main()