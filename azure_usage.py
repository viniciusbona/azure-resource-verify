#!/usr/bin/env python3
"""
Azure Inventory Script - Versão Completa com Classificação Corrigida para Public IPs
"""

import os
import json
import requests
import pandas as pd
import subprocess
import time
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List

from azure.identity import DefaultAzureCredential
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions

# ===== CONFIGURAÇÃO =====
DAYS = 30
OUT_DIR = "output"
SUBSCRIPTION_IDS = ["SDSSFAFSAFSDAS"]
SUBSCRIPTION_NAME = "sample"
REGION = "Brazil South"
DELAY_BETWEEN_REQUESTS = 2
ENABLE_METRICS = True
MAX_RETRIES = 3

os.makedirs(OUT_DIR, exist_ok=True)

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
        except Exception as e:
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

# ===== FUNÇÃO PARA DESCOBRIR TIPOS DE RECURSO COM SKU =====
def discover_sku_types(df: pd.DataFrame) -> List[str]:
    """Descobre dinamicamente quais tipos de recurso têm SKU"""
    sku_types = []
    unique_types = df['resourceType'].unique()
    
    print("\n🔍 Descobrindo tipos de recurso com SKU...")
    
    skip_types = [
        'microsoft.compute/restorepointcollections',
        'microsoft.operationalinsights/workspaces',
        'microsoft.network/networkinterfaces',
        'microsoft.network/networksecuritygroups',
        'microsoft.network/virtualnetworks',
        'microsoft.network/networkwatchers',
        'microsoft.network/routetables',
        'microsoft.compute/virtualmachines/extensions'
    ]
    
    for resource_type in unique_types:
        if resource_type in skip_types:
            continue
        
        sample = df[df['resourceType'] == resource_type]
        if sample.empty:
            continue
        sample = sample.iloc[0]
        
        try:
            if resource_type == 'microsoft.compute/virtualmachines':
                cmd = ["az", "vm", "show", "--ids", sample['id'], "--query", "hardwareProfile.vmSize", "--output", "tsv"]
            elif resource_type == 'microsoft.compute/disks':
                cmd = ["az", "disk", "show", "--ids", sample['id'], "--query", "sku.name", "--output", "tsv"]
            elif resource_type == 'microsoft.storage/storageaccounts':
                cmd = ["az", "storage", "account", "show", "--ids", sample['id'], "--query", "sku.name", "--output", "tsv"]
            elif resource_type == 'microsoft.network/publicipaddresses':
                cmd = ["az", "network", "public-ip", "show", "--ids", sample['id'], "--query", "sku.name", "--output", "tsv"]
            elif resource_type == 'microsoft.recoveryservices/vaults':
                cmd = ["az", "backup", "vault", "show", "--ids", sample['id'], "--query", "sku.name", "--output", "tsv"]
            else:
                cmd = ["az", "resource", "show", "--ids", sample['id'], "--query", "sku.name", "--output", "tsv"]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0 and result.stdout.strip():
                sku_types.append(resource_type)
                print(f"  ✅ {resource_type.split('/')[-1]} - tem SKU")
            else:
                print(f"  ⚠️ {resource_type.split('/')[-1]} - sem SKU")
        except Exception as e:
            print(f"  ⚠️ {resource_type.split('/')[-1]} - erro")
    
    return sku_types

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
        else:
            cmd = ["az", "resource", "show", "--ids", resource_id, "--query", "sku.name", "--output", "tsv"]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return ""

# ===== FUNÇÃO PARA OBTER MÉTRICAS DE VM =====
def get_vm_metrics_summary(resource_id: str, vm_sku: str = "") -> str:
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

# ===== FUNÇÃO PARA OBTER MÉTRICAS DE IP PÚBLICO =====
def get_public_ip_metrics_summary(resource_id: str) -> str:
    packet_count = get_metrics_with_retry(resource_id, "PacketCount", "Total", DAYS)
    if packet_count is not None and packet_count > 0:
        if packet_count >= 1000000:
            return f"{packet_count/1000000:.1f}M pacotes"
        elif packet_count >= 1000:
            return f"{packet_count/1000:.1f}K pacotes"
        return f"{packet_count:,.0f} pacotes"
    return "0 pacotes"

# ===== FUNÇÃO PARA OBTER MÉTRICAS DE STORAGE =====
def get_storage_metrics_summary(resource_id: str) -> str:
    transactions = get_metrics_with_retry(resource_id, "Transactions", "Total", DAYS)
    if transactions is not None and transactions > 0:
        if transactions >= 1000000:
            return f"Transações: {transactions/1000000:.1f}M"
        elif transactions >= 1000:
            return f"Transações: {transactions/1000:.1f}K"
        return f"Transações: {transactions:,.0f}"
    return "Sem transações"

# ===== FUNÇÃO PARA OBTER MÉTRICAS DE DISCO =====
def get_disk_metrics_summary(resource_id: str) -> str:
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

# ===== FUNÇÃO PARA OBTER MÉTRICAS DE NIC =====
def get_nic_metrics_summary(resource_id: str) -> str:
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

# ===== FUNÇÃO PARA OBTER MÉTRICAS DE NSG =====
def get_nsg_metrics_summary(resource_id: str) -> str:
    rules_hit = get_metrics_with_retry(resource_id, "AllRuleHits", "Total", DAYS)
    if rules_hit is not None and rules_hit > 0:
        if rules_hit >= 1000000:
            return f"{rules_hit/1000000:.1f}M regras acionadas"
        elif rules_hit >= 1000:
            return f"{rules_hit/1000:.1f}K regras acionadas"
        return f"{rules_hit:,.0f} regras acionadas"
    return "Sem regras acionadas"

# ===== FUNÇÃO PARA OBTER MÉTRICAS DE VNET =====
def get_vnet_usage_summary(resource_id: str, df: pd.DataFrame) -> str:
    summary_parts = []
    vnet_name = resource_id.split('/')[-1]
    
    associated_nics = df[
        (df['resourceType'] == 'microsoft.network/networkinterfaces') &
        (df['dependencies'].str.contains(vnet_name, na=False))
    ]
    if not associated_nics.empty:
        summary_parts.append(f"{len(associated_nics)} NIC(s) conectada(s)")
    
    gateways = df[
        (df['resourceType'].str.contains('gateway', na=False)) &
        (df['dependencies'].str.contains(vnet_name, na=False))
    ]
    if not gateways.empty:
        summary_parts.append(f"{len(gateways)} gateway(s) conectado(s)")
    
    private_endpoints = df[
        (df['resourceType'] == 'microsoft.network/privateendpoints') &
        (df['dependencies'].str.contains(vnet_name, na=False))
    ]
    if not private_endpoints.empty:
        summary_parts.append(f"{len(private_endpoints)} private endpoint(s)")
    
    if summary_parts:
        return "; ".join(summary_parts)
    return "Sem recursos anexados (VNet vazia)"

# ===== FUNÇÃO PARA OBTER MÉTRICAS DE BACKUP =====
def get_backup_metrics_summary(resource_id: str) -> str:
    restore_points = get_metrics_with_retry(resource_id, "Restore Point Count", "Total", DAYS)
    if restore_points is not None and restore_points > 0:
        return f"{restore_points:,.0f} pontos de restauração"
    return "Sem pontos de restauração"

# ===== FUNÇÃO PARA OBTER MÉTRICAS DE VAULT =====
def get_vault_metrics_summary(resource_id: str) -> str:
    backup_items = get_metrics_with_retry(resource_id, "Backup Items", "Average", DAYS)
    if backup_items is not None and backup_items > 0:
        return f"{backup_items:.0f} itens em backup"
    return "Sem itens em backup"

# ===== FUNÇÃO PARA OBTER MÉTRICAS DE LOG ANALYTICS =====
def get_log_analytics_metrics_summary(resource_id: str) -> str:
    ingested_data = get_metrics_with_retry(resource_id, "Data Ingestion", "Total", DAYS)
    if ingested_data is not None and ingested_data > 0:
        gb_ingested = ingested_data / (1024 * 1024 * 1024)
        return f"{gb_ingested:.1f} GB ingeridos"
    return "Sem dados ingeridos"

# ===== FUNÇÃO PARA BUSCAR TODOS OS RECURSOS =====
def get_all_resources(subscription_ids):
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

# ===== FUNÇÃO PARA OBTER CUSTO =====
def get_cost_for_resource(subscription_id, resource_id, resource_name, resource_type):
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

# ===== FUNÇÃO PARA ANALISAR RECURSO =====
def analyze_resource(resource_name: str, resource_type: str, cost: float, dependencies: str, usage_summary: str = "", sku: str = "") -> Dict:
    """Analisa recurso e retorna classificações"""
    
    if resource_type == 'microsoft.network/publicipaddresses':
        # Verificar se tem tráfego (pacotes)
        has_traffic = False
        if usage_summary and "pacotes" in usage_summary:
            try:
                if 'M' in usage_summary:
                    match = re.search(r'([\d.]+)M', usage_summary)
                    if match:
                        packet_count = float(match.group(1)) * 1000000
                        has_traffic = packet_count > 0
                elif 'K' in usage_summary:
                    match = re.search(r'([\d.]+)K', usage_summary)
                    if match:
                        packet_count = float(match.group(1)) * 1000
                        has_traffic = packet_count > 0
                else:
                    match = re.search(r'([\d,]+)', usage_summary)
                    if match:
                        packet_count = int(match.group(1).replace(',', ''))
                        has_traffic = packet_count > 0
            except:
                has_traffic = usage_summary != "0 pacotes"
        
        # Classificação baseada em uso real
        in_use = "Sim" if has_traffic else "Não"
        classification = "Rede - IP Público"
        purpose = "Public IP"
        
        # Determinar ação com base no tráfego e custo
        if not has_traffic:
            # IP sem tráfego
            if cost > 0:
                removal_impact = "Baixo"
                recommendation = f"PODE REMOVER - IP sem tráfego (custo R$ {cost:.2f})"
                orphan_candidate = "Sim"
            else:
                removal_impact = "Muito Baixo"
                recommendation = "PODE REMOVER - IP sem tráfego e sem custo"
                orphan_candidate = "Sim"
        else:
            # IP com tráfego
            if cost > 50:
                removal_impact = "Alto"
            elif cost > 10:
                removal_impact = "Médio"
            else:
                removal_impact = "Baixo"
            recommendation = "MANTER - IP em uso com tráfego detectado"
            orphan_candidate = "Não"
        
        return {
            'in_use': in_use,
            'classification': classification,
            'purpose': purpose,
            'removal_impact': removal_impact,
            'recommendation': recommendation,
            'orphan_candidate': orphan_candidate
        }
    
    # Análise padrão para outros recursos
    in_use = "Sim" if cost > 0 else "Não"
    
    if "virtualmachines" in resource_type:
        classification = "Computação - VM"
        purpose = "Virtual Machine"
    elif "disks" in resource_type:
        classification = "Armazenamento - Disco"
        purpose = "Managed Disk"
    elif "storageaccounts" in resource_type:
        classification = "Armazenamento - Storage"
        purpose = "Storage Account"
    elif "recoveryservices" in resource_type:
        classification = "Backup - Recovery Vault"
        purpose = "Backup Vault"
    elif "restorepointcollections" in resource_type:
        classification = "Backup - Pontos Restauração"
        purpose = "Backup Collection"
    elif "networkinterfaces" in resource_type:
        classification = "Rede - Interface"
        purpose = "Network Interface"
    elif "networksecuritygroups" in resource_type:
        classification = "Rede - Segurança"
        purpose = "NSG"
    elif "virtualnetworks" in resource_type:
        classification = "Rede - Virtual"
        purpose = "VNet"
    elif "operationalinsights" in resource_type:
        classification = "Monitoramento - Log Analytics"
        purpose = "Log Analytics"
    elif "networkwatchers" in resource_type:
        classification = "Monitoramento - Network Watcher"
        purpose = "Network Watcher"
    elif "routetables" in resource_type:
        classification = "Rede - Tabela de Rotas"
        purpose = "Route Table"
    elif "extensions" in resource_type:
        classification = "Computação - Extensão"
        purpose = "VM Extension"
    else:
        classification = "Outros"
        purpose = resource_type.split('/')[-1]
    
    removal_impact = "Baixo"
    if cost > 100:
        removal_impact = "Alto"
    elif cost > 10:
        removal_impact = "Médio"
    
    recommendation = "Manter"
    if cost == 0 and "Sem" in usage_summary:
        recommendation = "Revisar - Sem custo"
    
    orphan_candidate = "Não"
    if dependencies == "" and cost == 0:
        orphan_candidate = "Sim"
    
    return {
        'in_use': in_use,
        'classification': classification,
        'purpose': purpose,
        'removal_impact': removal_impact,
        'recommendation': recommendation,
        'orphan_candidate': orphan_candidate
    }

# ===== FUNÇÃO PRINCIPAL =====
def main():
    print("\n" + "="*60)
    print("🚀 AZURE INVENTORY SCRIPT - VERSÃO COMPLETA")
    print("="*60)
    
    # 1. Buscar recursos
    df = get_all_resources(SUBSCRIPTION_IDS)
    if df.empty:
        print("\n❌ Nenhum recurso encontrado!")
        exit(1)
    
    print(f"\n📊 Total de recursos encontrados: {len(df)}")
    
    # 2. Estatísticas por tipo
    print("\n📊 Distribuição por tipo de recurso:")
    type_counts = df['resourceType'].value_counts()
    for rt, count in type_counts.head(15).items():
        type_short = rt.split('/')[-1]
        print(f"  • {type_short:<35} {count:>2} recursos")
    
    # 3. Resetar índice para evitar duplicatas
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
    
    # 5. Obter SKU
    sku_types = discover_sku_types(df)
    print("\n🔄 OBTENDO SKU...")
    for idx, row in df.iterrows():
        if row['resourceType'] in sku_types:
            df.loc[idx, 'skuName'] = get_resource_sku_dynamic(row['id'], row['resourceType'])
    
    # 6. Identificar dependências
    print("\n🔗 IDENTIFICANDO DEPENDÊNCIAS...")
    df = identify_dependencies(df)
    
    # 7. Coletar métricas e custos
    print("\n💰 COLETANDO MÉTRICAS E CUSTOS...")
    
    cost_types = ['microsoft.compute/virtualmachines', 'microsoft.compute/disks',
                  'microsoft.storage/storageaccounts', 'microsoft.network/publicipaddresses',
                  'microsoft.recoveryservices/vaults', 'microsoft.operationalinsights/workspaces']
    
    for idx, (idx_row, row) in enumerate(df.iterrows()):
        resource_type = row['resourceType']
        resource_name = row['resourceName']
        type_short = resource_type.split('/')[-1]
        
        print(f"\n[{idx+1}/{len(df)}] {resource_name[:45]} ({type_short})")
        
        # Coletar métricas
        summary = ""
        try:
            if resource_type == 'microsoft.compute/virtualmachines':
                summary = get_vm_metrics_summary(row['id'], row['skuName'])
                print(f"   📊 VM: {summary[:80]}")
            elif resource_type == 'microsoft.network/publicipaddresses':
                summary = get_public_ip_metrics_summary(row['id'])
                print(f"   📊 IP: {summary}")
            elif resource_type == 'microsoft.storage/storageaccounts':
                summary = get_storage_metrics_summary(row['id'])
                print(f"   📊 Storage: {summary}")
            elif resource_type == 'microsoft.compute/disks':
                summary = get_disk_metrics_summary(row['id'])
                print(f"   📊 Disco: {summary}")
            elif resource_type == 'microsoft.network/networkinterfaces':
                summary = get_nic_metrics_summary(row['id'])
                print(f"   📊 NIC: {summary}")
            elif resource_type == 'microsoft.network/networksecuritygroups':
                summary = get_nsg_metrics_summary(row['id'])
                print(f"   📊 NSG: {summary}")
            elif resource_type == 'microsoft.network/virtualnetworks':
                summary = get_vnet_usage_summary(row['id'], df)
                print(f"   📊 VNet: {summary}")
            elif resource_type == 'microsoft.compute/restorepointcollections':
                summary = get_backup_metrics_summary(row['id'])
                print(f"   📊 Backup: {summary}")
            elif resource_type == 'microsoft.recoveryservices/vaults':
                summary = get_vault_metrics_summary(row['id'])
                print(f"   📊 Vault: {summary}")
            elif resource_type == 'microsoft.operationalinsights/workspaces':
                summary = get_log_analytics_metrics_summary(row['id'])
                print(f"   📊 Log Analytics: {summary}")
            elif resource_type == 'microsoft.network/networkwatchers':
                summary = "Monitoramento de rede ativo"
                print(f"   📊 Network Watcher: {summary}")
            elif resource_type == 'microsoft.network/routetables':
                summary = "Tabela de rotas configurada"
                print(f"   📊 Route Table: {summary}")
            elif 'extensions' in resource_type:
                summary = "Extensão de VM instalada"
                print(f"   📊 Extension: {summary}")
            else:
                summary = f"Recurso do tipo {type_short}"
                print(f"   📊 {summary}")
        except Exception as e:
            summary = f"Erro: {str(e)[:50]}"
            print(f"   ⚠️ {summary}")
        
        df.loc[idx_row, 'usage_summary'] = summary if summary else "Sem métricas disponibles"
        
        # Coletar custo
        if resource_type in cost_types:
            cost = get_cost_for_resource(row['subscriptionId'], row['id'], row['resourceName'], resource_type)
            if cost is not None:
                df.loc[idx_row, 'cost_30d'] = round(cost, 4)
                df.loc[idx_row, 'daily_cost_30d'] = round(cost / DAYS, 4)
                if cost > 0:
                    print(f"   💰 Custo: R$ {cost:.2f}")
                else:
                    print(f"   💰 Custo: R$ 0.00")
    
    # 8. Classificação
    print("\n📊 CLASSIFICANDO RECURSOS...")
    
    classifications = []
    for idx, row in df.iterrows():
        analysis = analyze_resource(
            row['resourceName'],
            row['resourceType'],
            row['cost_30d'],
            row.get('dependencies', ''),
            row.get('usage_summary', ''),
            row.get('skuName', '')
        )
        classifications.append(analysis)
    
    analysis_df = pd.DataFrame(classifications)
    df = pd.concat([df, analysis_df], axis=1)
    
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
        with_summary = len(df[(df['resourceType'] == resource_type) & (df['usage_summary'] != "")])
        print(f"  • {type_short:<35} {count:>2} recursos - {with_summary:>2} com métricas")
    
    # Estatísticas específicas para Public IPs
    print(f"\n🌐 ANÁLISE DE IPs PÚBLICOS:")
    
    # Criar listas separadas para evitar problemas de pandas
    ips_com_trafego = []
    ips_sem_trafego = []
    
    for idx, row in df.iterrows():
        if row['resourceType'] == 'microsoft.network/publicipaddresses':
            in_use_value = row.get('in_use', 'Não')
            # Garantir que in_use_value é string
            if isinstance(in_use_value, str):
                in_use_str = in_use_value
            else:
                in_use_str = str(in_use_value) if in_use_value is not None else 'Não'
            
            ip_info = {
                'resourceName': row['resourceName'],
                'in_use': in_use_str,
                'cost_30d': row.get('cost_30d', 0),
                'usage_summary': row.get('usage_summary', '')
            }
            
            if in_use_str == 'Sim':
                ips_com_trafego.append(ip_info)
            else:
                ips_sem_trafego.append(ip_info)
    
    print(f"  • IPs com tráfego: {len(ips_com_trafego)}")
    print(f"  • IPs sem tráfego: {len(ips_sem_trafego)}")
    
    if ips_sem_trafego:
        print(f"\n  🗑️ IPs candidatos à remoção:")
        for ip in ips_sem_trafego:
            print(f"     - {ip['resourceName']} (custo: R$ {ip['cost_30d']:.2f}) - {ip['usage_summary']}")
    
    print(f"\n💾 Arquivo salvo: {output_file}")
    print(f"\n✅ Processamento concluído!")

if __name__ == "__main__":
    main()