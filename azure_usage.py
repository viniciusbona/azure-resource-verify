#!/usr/bin/env python3
"""
Azure Inventory Script - Versão Completa com Suporte a TODOS os tipos de recursos
Inclui métricas para: VMs, Discos, IPs, NICs, NSGs, VNets, Storage, SQL, Key Vault,
App Services, Service Bus, Cosmos DB, AKS, Load Balancers, Logic Apps, API Management, etc.
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

os.makedirs(OUT_DIR, exist_ok=True)

# ===== FUNÇÃO PARA OBTER NOMES DAS SUBSCRIPTIONS =====
def get_subscription_names(subscription_ids):
    """Obtém os nomes das subscriptions usando Azure CLI"""
    subscription_names = {}
    
    try:
        # Tentar obter via Azure CLI
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
        print(f"⚠️ Erro ao obter nomes das subscriptions via CLI: {e}")
    
    # Para subscriptions não encontradas, usar o ID truncado
    for sub_id in subscription_ids:
        if sub_id not in subscription_names:
            subscription_names[sub_id] = sub_id[:8]
    
    return subscription_names

# ===== FUNÇÃO PARA VALIDAR SUBSCRIPTION IDS =====
def validate_subscription_ids(subscription_ids):
    """Valida se os subscription IDs são válidos e acessíveis"""
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
        print(f"\n⚠️ ATENÇÃO: As seguintes subscriptions são inválidas ou inacessíveis:")
        for inv_id in invalid_ids:
            print(f"   - {inv_id}")
    
    return valid_ids, invalid_ids

# ===== INICIALIZAÇÃO =====
print("🔧 Inicializando...")
credential = DefaultAzureCredential()
arg_client = ResourceGraphClient(credential)

try:
    token = credential.get_token("https://management.azure.com/.default").token
    print("✅ Token obtido com sucesso")
except Exception as e:
    print(f"❌ Erro ao obter token: {e}")
    exit(1)

end = datetime.now(timezone.utc)
start = end - timedelta(days=DAYS)

print(f"Período análise: {start.date()} → {end.date()}")

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

# ===== FUNÇÃO PARA FORMATAR NÚMEROS GRANDES =====
def format_number(value: float) -> str:
    """Formata números grandes com K, M, B"""
    if value >= 1_000_000_000:
        return f"{value/1_000_000_000:.1f}B"
    elif value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value/1_000:.1f}K"
    return f"{value:,.0f}"

# ===== FUNÇÕES DE MÉTRICAS POR TIPO DE RECURSO =====

def get_vm_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas da VM"""
    summary_parts = []
    cpu_avg = get_metrics_with_retry(resource_id, "Percentage CPU", "Average", DAYS)
    if cpu_avg is not None and cpu_avg > 0:
        summary_parts.append(f"CPU: {cpu_avg:.1f}%")
    net_in = get_metrics_with_retry(resource_id, "Network In Total", "Total", DAYS)
    net_out = get_metrics_with_retry(resource_id, "Network Out Total", "Total", DAYS)
    if net_in is not None or net_out is not None:
        total_mb = (net_in or 0) / 1024 / 1024 + (net_out or 0) / 1024 / 1024
        if total_mb > 0:
            if total_mb >= 1024:
                summary_parts.append(f"Rede: {total_mb/1024:.1f} GB")
            else:
                summary_parts.append(f"Rede: {total_mb:.1f} MB")
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

def get_public_ip_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do Public IP usando Packet Count"""
    packet_count = get_metrics_with_retry(resource_id, "PacketCount", "Total", DAYS)
    if packet_count is not None and packet_count > 0:
        return f"{format_number(packet_count)} pacotes"
    return "0 pacotes"

def get_storage_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do Storage Account"""
    transactions = get_metrics_with_retry(resource_id, "Transactions", "Total", DAYS)
    if transactions is not None and transactions > 0:
        return f"Transações: {format_number(transactions)}"
    return "Sem transações"

def get_disk_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do Disco"""
    read_ops = get_metrics_with_retry(resource_id, "Composite Disk Read Operations/sec", "Average", DAYS)
    write_ops = get_metrics_with_retry(resource_id, "Composite Disk Write Operations/sec", "Average", DAYS)
    summary_parts = []
    if read_ops is not None and read_ops > 0:
        if read_ops >= 1000:
            summary_parts.append(f"Leitura: {read_ops/1000:.1f}K ops/s")
        else:
            summary_parts.append(f"Leitura: {read_ops:.1f} ops/s")
    if write_ops is not None and write_ops > 0:
        if write_ops >= 1000:
            summary_parts.append(f"Escrita: {write_ops/1000:.1f}K ops/s")
        else:
            summary_parts.append(f"Escrita: {write_ops:.1f} ops/s")
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

def get_nic_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas da Interface de Rede"""
    summary_parts = []
    bytes_received = get_metrics_with_retry(resource_id, "Bytes Received", "Total", DAYS)
    if bytes_received is not None and bytes_received > 0:
        mb_received = bytes_received / (1024 * 1024)
        if mb_received >= 1024:
            summary_parts.append(f"Rx: {mb_received/1024:.1f} GB")
        else:
            summary_parts.append(f"Rx: {mb_received:.1f} MB")
    bytes_sent = get_metrics_with_retry(resource_id, "Bytes Sent", "Total", DAYS)
    if bytes_sent is not None and bytes_sent > 0:
        mb_sent = bytes_sent / (1024 * 1024)
        if mb_sent >= 1024:
            summary_parts.append(f"Tx: {mb_sent/1024:.1f} GB")
        else:
            summary_parts.append(f"Tx: {mb_sent:.1f} MB")
    return "; ".join(summary_parts) if summary_parts else "Sem tráfego"

def get_nsg_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do NSG"""
    rules_hit = get_metrics_with_retry(resource_id, "AllRuleHits", "Total", DAYS)
    if rules_hit is not None and rules_hit > 0:
        return f"{format_number(rules_hit)} regras acionadas"
    return "Sem regras acionadas"

def get_vnet_usage_summary(resource_id: str, df: pd.DataFrame) -> str:
    """Verifica uso da VNet baseado em dispositivos anexados"""
    summary_parts = []
    vnet_name = resource_id.split('/')[-1]
    
    associated_nics = df[
        (df['resourceType'] == 'microsoft.network/networkinterfaces') &
        (df['dependencies'].str.contains(vnet_name, na=False))
    ]
    if not associated_nics.empty:
        summary_parts.append(f"{len(associated_nics)} NIC(s) conectada(s)")
    
    if summary_parts:
        return "; ".join(summary_parts)
    return "Sem recursos anexados (VNet vazia)"

def get_backup_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas da Backup Collection"""
    restore_points = get_metrics_with_retry(resource_id, "Restore Point Count", "Total", DAYS)
    if restore_points is not None and restore_points > 0:
        return f"{restore_points:,.0f} pontos de restauração"
    return "Sem pontos de restauração"

def get_vault_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do Recovery Vault"""
    backup_items = get_metrics_with_retry(resource_id, "Backup Items", "Average", DAYS)
    if backup_items is not None and backup_items > 0:
        return f"{backup_items:.0f} itens em backup"
    return "Sem itens em backup"

def get_log_analytics_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do Log Analytics"""
    ingested_data = get_metrics_with_retry(resource_id, "Data Ingestion", "Total", DAYS)
    if ingested_data is not None and ingested_data > 0:
        gb_ingested = ingested_data / (1024 * 1024 * 1024)
        return f"{gb_ingested:.1f} GB ingeridos"
    return "Sem dados ingeridos"

def get_app_service_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do App Service / Function App"""
    summary_parts = []
    requests = get_metrics_with_retry(resource_id, "Requests", "Total", DAYS)
    if requests is not None and requests > 0:
        summary_parts.append(f"Requisições: {format_number(requests)}")
    cpu = get_metrics_with_retry(resource_id, "CpuPercentage", "Average", DAYS)
    if cpu is not None and cpu > 0:
        summary_parts.append(f"CPU: {cpu:.1f}%")
    memory = get_metrics_with_retry(resource_id, "MemoryWorkingSet", "Average", DAYS)
    if memory is not None and memory > 0:
        memory_gb = memory / (1024 * 1024 * 1024)
        summary_parts.append(f"Memória: {memory_gb:.1f} GB")
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

def get_sql_db_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do SQL Database"""
    summary_parts = []
    dtu = get_metrics_with_retry(resource_id, "dtu_consumption_percent", "Average", DAYS)
    if dtu is not None and dtu > 0:
        summary_parts.append(f"DTU: {dtu:.1f}%")
    cpu = get_metrics_with_retry(resource_id, "cpu_percent", "Average", DAYS)
    if cpu is not None and cpu > 0:
        summary_parts.append(f"CPU: {cpu:.1f}%")
    storage = get_metrics_with_retry(resource_id, "storage_percent", "Average", DAYS)
    if storage is not None and storage > 0:
        summary_parts.append(f"Storage: {storage:.1f}%")
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

def get_keyvault_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do Key Vault"""
    summary_parts = []
    requests = get_metrics_with_retry(resource_id, "ServiceApiHit", "Total", DAYS)
    if requests is not None and requests > 0:
        summary_parts.append(f"Requisições: {format_number(requests)}")
    latency = get_metrics_with_retry(resource_id, "ServiceApiLatency", "Average", DAYS)
    if latency is not None and latency > 0:
        summary_parts.append(f"Latência: {latency:.0f}ms")
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

def get_servicebus_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do Service Bus"""
    summary_parts = []
    incoming = get_metrics_with_retry(resource_id, "IncomingMessages", "Total", DAYS)
    if incoming is not None and incoming > 0:
        summary_parts.append(f"Msg recebidas: {format_number(incoming)}")
    outgoing = get_metrics_with_retry(resource_id, "OutgoingMessages", "Total", DAYS)
    if outgoing is not None and outgoing > 0:
        summary_parts.append(f"Msg enviadas: {format_number(outgoing)}")
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

def get_cosmosdb_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do Cosmos DB"""
    summary_parts = []
    requests = get_metrics_with_retry(resource_id, "TotalRequests", "Total", DAYS)
    if requests is not None and requests > 0:
        summary_parts.append(f"Requisições: {format_number(requests)}")
    ru = get_metrics_with_retry(resource_id, "TotalRequestUnits", "Total", DAYS)
    if ru is not None and ru > 0:
        summary_parts.append(f"RU: {format_number(ru)}")
    storage = get_metrics_with_retry(resource_id, "DataUsage", "Average", DAYS)
    if storage is not None and storage > 0:
        storage_gb = storage / (1024 * 1024 * 1024)
        summary_parts.append(f"Storage: {storage_gb:.1f} GB")
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

def get_loadbalancer_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do Load Balancer"""
    summary_parts = []
    bytes_processed = get_metrics_with_retry(resource_id, "ByteCount", "Total", DAYS)
    if bytes_processed is not None and bytes_processed > 0:
        gb_processed = bytes_processed / (1024 * 1024 * 1024)
        summary_parts.append(f"Dados: {gb_processed:.1f} GB")
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

def get_logicapp_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do Logic App"""
    summary_parts = []
    runs = get_metrics_with_retry(resource_id, "RunsStarted", "Total", DAYS)
    if runs is not None and runs > 0:
        summary_parts.append(f"Execuções: {format_number(runs)}")
    failed = get_metrics_with_retry(resource_id, "RunsFailed", "Total", DAYS)
    if failed is not None and failed > 0:
        summary_parts.append(f"Falhas: {format_number(failed)}")
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

def get_apim_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do API Management"""
    summary_parts = []
    requests = get_metrics_with_retry(resource_id, "Requests", "Total", DAYS)
    if requests is not None and requests > 0:
        summary_parts.append(f"Requisições: {format_number(requests)}")
    duration = get_metrics_with_retry(resource_id, "Duration", "Average", DAYS)
    if duration is not None and duration > 0:
        summary_parts.append(f"Duração: {duration:.0f}ms")
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

def get_aks_metrics_summary(resource_id: str) -> str:
    """Retorna resumo das métricas do AKS Cluster"""
    summary_parts = []
    nodes = get_metrics_with_retry(resource_id, "nodesCount", "Average", DAYS)
    if nodes is not None and nodes > 0:
        summary_parts.append(f"Nós: {nodes:.0f}")
    return "; ".join(summary_parts) if summary_parts else "Sem métricas"

# ===== FUNÇÃO PARA BUSCAR TODOS OS RECURSOS =====
def get_all_resources(subscription_ids, subscription_names):
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
        sub_name = subscription_names.get(sub_id, sub_id[:8])
        print(f"\n📡 Buscando recursos na subscription: {sub_name} ({sub_id[:8]}...)")
        
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
        'networkwatchers': ("Monitoramento - Network Watcher", "Network Watcher"),
        'routetables': ("Rede - Tabela de Rotas", "Route Table"),
        'loadbalancers': ("Rede - Load Balancer", "Load Balancer"),
        'virtualnetworkgateways': ("Rede - VPN Gateway", "VPN Gateway"),
        'bastionhosts': ("Rede - Bastion", "Bastion"),
        'privateendpoints': ("Rede - Private Endpoint", "Private Endpoint"),
        'sql/servers/databases': ("Banco de Dados - SQL", "SQL Database"),
        'keyvault': ("Segurança - Key Vault", "Key Vault"),
        'sites': ("Aplicação - App Service", "App Service"),
        'serverfarms': ("Aplicação - App Service Plan", "App Service Plan"),
        'logic': ("Integração - Logic App", "Logic App"),
        'servicebus': ("Integração - Service Bus", "Service Bus"),
        'documentdb': ("Banco de Dados - Cosmos DB", "Cosmos DB"),
        'containerservice': ("Contêiner - Kubernetes", "AKS"),
        'containerregistry': ("Contêiner - Container Registry", "Container Registry"),
        'databricks': ("Análise - Databricks", "Databricks"),
        'powerbidedicated': ("BI - Power BI Embedded", "Power BI"),
        'automation': ("Automação - Automation Account", "Automation"),
        'apimanagement': ("Integração - API Management", "API Management"),
        'dnszones': ("Rede - DNS Zone", "DNS Zone"),
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
        elif resource_type == 'microsoft.network/networkinterfaces':
            nic_name = resource_name
            for _, vm in df[df['resourceType'] == 'microsoft.compute/virtualmachines'].iterrows():
                if nic_name in vm['resourceName'].lower():
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
        elif resource_type == 'microsoft.compute/restorepointcollections':
            backup_name = resource_name
            for _, vm in df[df['resourceType'] == 'microsoft.compute/virtualmachines'].iterrows():
                if vm['resourceName'].lower() in backup_name.lower():
                    dependencies.append(f"VM: {vm['resourceName']}")
                    attached.append(f"VM: {vm['resourceName']}")
                    break
        
        if dependencies:
            df.at[idx, 'dependencies'] = "; ".join(set(dependencies))
        if attached:
            df.at[idx, 'attached_resources'] = "; ".join(set(attached))
        if parent:
            df.at[idx, 'parent_resource'] = parent
    return df

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
        else:
            cmd = ["az", "resource", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return ""

# ===== FUNÇÃO PARA OBTER CUSTO =====
def get_cost_for_resource(subscription_id, resource_id, resource_name, resource_type):
    """Obtém custo para um recurso"""
    url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
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
        if response.status_code != 200:
            return None
        data = response.json()
        rows = data.get("properties", {}).get("rows", [])
        if rows and len(rows) > 0:
            return float(rows[0][0]) if rows[0][0] else 0.0
        return 0.0
    except Exception:
        return None

# ===== FUNÇÃO PRINCIPAL =====
def main():
    parser = argparse.ArgumentParser(description='Azure Inventory Script - Coleta de recursos e métricas')
    parser.add_argument('--subscription-ids', nargs='+', 
                        help='IDs das subscriptions para análise (ex: --subscription-ids "id1" "id2")')
    args = parser.parse_args()
    
    # Obter subscription IDs dos argumentos ou usar padrão
    if args.subscription_ids:
        subscription_ids = args.subscription_ids
        print(f"\n📋 Usando subscriptions fornecidas: {len(subscription_ids)}")
    else:
        # Subscription padrão (primeira da lista)
        default_subscriptions = ["977408fa-9ef9-4bd6-bbfd-8fb6f8cc0550"]
        subscription_ids = default_subscriptions
        print(f"\n📋 Nenhuma subscription fornecida. Usando subscription padrão: {default_subscriptions[0][:8]}...")
        print("   Para especificar subscriptions, use: --subscription-ids \"id1\" \"id2\"")
    
    # Validar subscriptions
    valid_ids, invalid_ids = validate_subscription_ids(subscription_ids)
    
    if not valid_ids:
        print("\n❌ Nenhuma subscription válida encontrada!")
        exit(1)
    
    if invalid_ids:
        print(f"\n⚠️ {len(invalid_ids)} subscription(s) inválida(s) ignorada(s)")
    
    # Obter nomes das subscriptions dinamicamente
    subscription_names = get_subscription_names(valid_ids)
    
    print("\n" + "="*60)
    print("🚀 AZURE INVENTORY SCRIPT - VERSÃO COMPLETA (TODOS OS TIPOS)")
    print("="*60)
    
    print(f"\n📋 Subscriptions a serem analisadas:")
    for sub_id in valid_ids:
        print(f"  • {subscription_names.get(sub_id, sub_id[:8])} ({sub_id})")
    
    # 1. Buscar TODOS os recursos
    df = get_all_resources(valid_ids, subscription_names)
    if df.empty:
        print("\n❌ Nenhum recurso encontrado!")
        exit(1)
    
    print(f"\n📊 Total de recursos encontrados: {len(df)}")
    
    # 2. Estatísticas por tipo
    print("\n📊 Distribuição por tipo de recurso:")
    type_counts = df['resourceType'].value_counts()
    for rt, count in type_counts.head(30).items():
        type_short = rt.split('/')[-1]
        print(f"  • {type_short:<50} {count:>3} recursos")
    
    # 3. Resetar índice
    df = df.reset_index(drop=True)
    
    # 4. Adicionar colunas
    df['skuName'] = ""
    df['kindName'] = ""
    df['managedBy'] = ""
    df['parent_resource'] = ""
    df['attached_resources'] = ""
    df['dependencies'] = ""
    df['daily_cost_30d'] = 0.0
    df['cost_30d'] = 0.0
    df['usage_summary'] = ""
    df['in_use'] = ""
    df['classification'] = ""
    df['purpose'] = ""
    df['removal_impact'] = ""
    df['recommendation'] = ""
    df['orphan_candidate'] = ""
    
    # 5. Identificar dependências
    print("\n🔗 IDENTIFICANDO DEPENDÊNCIAS...")
    df = identify_dependencies(df)
    
    # 6. Obter SKU para recursos que têm SKU
    print("\n🔄 OBTENDO SKU...")
    sku_types = [
        'microsoft.compute/virtualmachines',
        'microsoft.compute/disks',
        'microsoft.storage/storageaccounts',
        'microsoft.network/publicipaddresses',
        'microsoft.recoveryservices/vaults',
        'microsoft.web/serverfarms',
        'microsoft.containerservice/managedclusters'
    ]
    for idx, row in df.iterrows():
        if idx % 20 == 0:
            print(f"  Processando SKU: {idx + 1}/{len(df)}")
        if row['resourceType'] in sku_types:
            df.loc[idx, 'skuName'] = get_resource_sku_dynamic(row['id'], row['resourceType'])
    
    # 7. Coletar métricas e custos
    print("\n💰 COLETANDO MÉTRICAS E CUSTOS...")
    
    cost_success = 0
    metric_success = 0
    
    for idx, (idx_row, row) in enumerate(df.iterrows()):
        resource_type = row['resourceType']
        resource_name = row['resourceName']
        type_short = resource_type.split('/')[-1]
        
        if idx % 20 == 0:
            print(f"\nProcessando recurso {idx+1}/{len(df)}: {resource_name[:50]} ({type_short})")
        
        # Coletar métricas por tipo
        summary = ""
        try:
            if resource_type == 'microsoft.compute/virtualmachines':
                summary = get_vm_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.network/publicipaddresses':
                summary = get_public_ip_metrics_summary(row['id'])
                metric_success += 1
            elif resource_type == 'microsoft.storage/storageaccounts':
                summary = get_storage_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.compute/disks':
                summary = get_disk_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.network/networkinterfaces':
                summary = get_nic_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.network/networksecuritygroups':
                summary = get_nsg_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.network/virtualnetworks':
                summary = get_vnet_usage_summary(row['id'], df)
                metric_success += 1
            elif resource_type == 'microsoft.compute/restorepointcollections':
                summary = get_backup_metrics_summary(row['id'])
                metric_success += 1
            elif resource_type == 'microsoft.recoveryservices/vaults':
                summary = get_vault_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.operationalinsights/workspaces':
                summary = get_log_analytics_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.web/sites':
                summary = get_app_service_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.sql/servers/databases':
                summary = get_sql_db_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.keyvault/vaults':
                summary = get_keyvault_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.servicebus/namespaces':
                summary = get_servicebus_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.documentdb/databaseaccounts':
                summary = get_cosmosdb_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.network/loadbalancers':
                summary = get_loadbalancer_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.logic/workflows':
                summary = get_logicapp_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.apimanagement/service':
                summary = get_apim_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.containerservice/managedclusters':
                summary = get_aks_metrics_summary(row['id'])
                if summary and "Sem" not in summary:
                    metric_success += 1
            elif resource_type == 'microsoft.network/networkwatchers':
                summary = "Monitoramento de rede ativo"
                metric_success += 1
            elif resource_type == 'microsoft.network/routetables':
                summary = "Tabela de rotas configurada"
                metric_success += 1
            elif 'extensions' in resource_type:
                summary = "Extensão de VM instalada"
                metric_success += 1
            else:
                summary = f"Recurso do tipo {type_short}"
        except Exception as e:
            summary = f"Erro: {str(e)[:50]}"
        
        df.loc[idx_row, 'usage_summary'] = summary if summary else "Sem métricas disponíveis"
        
        # Coletar custo
        cost = get_cost_for_resource(row['subscriptionId'], row['id'], row['resourceName'], resource_type)
        if cost is not None:
            df.loc[idx_row, 'cost_30d'] = round(cost, 4)
            df.loc[idx_row, 'daily_cost_30d'] = round(cost / DAYS, 4)
            cost_success += 1
    
    # 8. Classificação
    print("\n📊 CLASSIFICANDO RECURSOS...")
    
    for idx, row in df.iterrows():
        classification, purpose = classify_resource(row['resourceType'])
        cost = row['cost_30d']
        in_use = "Sim" if cost > 0 else "Não"
        
        removal_impact = "Baixo"
        if cost > 100:
            removal_impact = "Alto"
        elif cost > 10:
            removal_impact = "Médio"
        
        recommendation = "Manter"
        if cost == 0:
            recommendation = "Revisar - Sem custo"
        
        orphan_candidate = "Não"
        if row.get('dependencies', '') == "" and cost == 0:
            orphan_candidate = "Sim"
        
        df.loc[idx, 'in_use'] = in_use
        df.loc[idx, 'classification'] = classification
        df.loc[idx, 'purpose'] = purpose
        df.loc[idx, 'removal_impact'] = removal_impact
        df.loc[idx, 'recommendation'] = recommendation
        df.loc[idx, 'orphan_candidate'] = orphan_candidate
    
    # 9. Salvar CSV
    required_columns = [
        'id', 'subscriptionId', 'resourceName', 'resourceType', 'resourceGroup', 'region',
        'skuName', 'kindName', 'managedBy', 'parent_resource', 'attached_resources', 'dependencies',
        'daily_cost_30d', 'cost_30d', 'usage_summary', 'in_use', 'classification', 'purpose',
        'removal_impact', 'recommendation', 'orphan_candidate', 'tags'
    ]
    for col in required_columns:
        if col not in df.columns:
            df[col] = ""
    
    df = df[required_columns]
    output_file = f"{OUT_DIR}/azure_inventory_complete.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # 10. Estatísticas finais
    print("\n" + "="*60)
    print("📊 ESTATÍSTICAS FINAIS")
    print("="*60)
    
    total_cost = df['cost_30d'].sum()
    print(f"💰 Custo total (30 dias): R$ {total_cost:,.2f}")
    
    print(f"\n📊 Resumo por tipo de recurso:")
    for resource_type in df['resourceType'].unique():
        type_short = resource_type.split('/')[-1]
        count = len(df[df['resourceType'] == resource_type])
        with_metrics = len(df[(df['resourceType'] == resource_type) & (~df['usage_summary'].str.contains("Sem", na=False))])
        print(f"  • {type_short:<45} {count:>3} recursos - {with_metrics:>3} com métricas")
    
    print(f"\n📈 RESUMO DA COLETA:")
    print(f"  ✅ Custos coletados: {cost_success}/{len(df)} recursos")
    print(f"  ✅ Métricas coletadas: {metric_success} recursos")
    
    print(f"\n💾 Arquivo salvo: {output_file}")
    print(f"\n✅ Processamento concluído!")

if __name__ == "__main__":
    main()