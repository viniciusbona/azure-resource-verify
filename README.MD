# 📊 Azure Inventory Script

## Visão Geral

Script Python para inventário completo de recursos Azure, coletando métricas de uso, custos e identificando recursos órfãos e candidatos à otimização de custos.

---

## ✨ Funcionalidades

- 🔍 **Descoberta automática** de todos os recursos em múltiplas subscriptions
- 📈 **Coleta de métricas** específicas por tipo de recurso (CPU, rede, transações, etc.)
- 💰 **Cálculo de custos** dos últimos 30 dias via Cost Management API
- 🔗 **Identificação de dependências** entre recursos (VM ↔ Disco ↔ NIC ↔ IP)
- 🏷️ **Classificação automática** por categoria e finalidade
- 🗑️ **Detecção de recursos órfãos** candidatos à remoção
- 📊 **Geração de relatório CSV** com todas as informações

---

## 🚀 Como Executar

### 1. Pré-requisitos

- Python 3.8 ou superior
- Azure CLI instalado e configurado
- Acesso de leitura às subscriptions

### 2. Configuração do Ambiente

```bash
# Criar ambiente virtual
python3 -m venv venv

# Ativar ambiente virtual (Linux/Mac)
source venv/bin/activate

# Ativar ambiente virtual (Windows)
venv\Scripts\activate

# Instalar dependências
pip install -r requirements.txt
```


### 3. Login Azure

```bash
az login
```

### 4. Executar o script

```bash
# Com uma subscription padrão
python3 azure_inventory.py

# Com subscriptions específicas
python3 azure_inventory.py --subscription-ids "977408fa-9ef9-4bd6-bbfd-8fb6f8cc0550" "fb61a2b6-5478-488a-a5e6-d123b28d30d9"

# Ajuda
python3 azure_inventory.py --help

```

# 📊 Documentação Técnica - Azure Inventory Script

## 🔧 Tipos de Recursos Suportados

### Computação
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.compute/virtualmachines` | Máquinas Virtuais Windows/Linux |
| `microsoft.compute/virtualmachines/extensions` | Extensões de VM |
| `microsoft.compute/virtualMachineScaleSets` | Conjuntos de escala de VMs |

### Armazenamento
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.compute/disks` | Discos gerenciados (SSD Premium, Standard HDD) |
| `microsoft.storage/storageaccounts` | Contas de armazenamento (Blob, File, Table, Queue) |

### Rede
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.network/publicipaddresses` | Endereços IP públicos (Basic/Standard) |
| `microsoft.network/networkinterfaces` | Interfaces de rede (NICs) |
| `microsoft.network/networksecuritygroups` | Grupos de segurança de rede (NSGs) |
| `microsoft.network/virtualnetworks` | Redes virtuais (VNets) |
| `microsoft.network/loadbalancers` | Balanceadores de carga |
| `microsoft.network/virtualnetworkgateways` | Gateways VPN |
| `microsoft.network/bastionhosts` | Hosts Bastion |
| `microsoft.network/privateendpoints` | Endpoints privados |
| `microsoft.network/dnszones` | Zonas DNS |
| `microsoft.network/routeTables` | Tabelas de rotas |

### Banco de Dados
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.sql/servers/databases` | Bancos de dados SQL |
| `microsoft.documentdb/databaseaccounts` | Contas Cosmos DB |

### Aplicações
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.web/sites` | App Services / Function Apps |
| `microsoft.web/serverfarms` | Planos de App Service |
| `microsoft.logic/workflows` | Logic Apps |
| `microsoft.apimanagement/service` | API Management |

### Integração
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.servicebus/namespaces` | Namespaces Service Bus |
| `microsoft.web/connections` | Conexões de API |

### Segurança
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.keyvault/vaults` | Key Vaults |

### Contêineres
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.containerservice/managedclusters` | Clusters AKS |
| `microsoft.containerregistry/registries` | Container Registry |

### Monitoramento
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.operationalinsights/workspaces` | Workspaces Log Analytics |
| `microsoft.insights/components` | Application Insights |
| `microsoft.network/networkwatchers` | Network Watcher |

### Backup
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.recoveryservices/vaults` | Recovery Services Vaults |
| `microsoft.compute/restorepointcollections` | Coleções de pontos de restauração |

### BI e Análise
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.powerbidedicated/capacities` | Capacidades Power BI Embedded |
| `microsoft.databricks/workspaces` | Workspaces Databricks |

### Automação
| Tipo de Recurso | Descrição |
|-----------------|-----------|
| `microsoft.automation/automationAccounts` | Contas de automação |

---

## 📈 Métricas Coletadas

### Virtual Machines
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `Percentage CPU` | Average | % | Percentual de uso da CPU |
| `Network In Total` | Total | Bytes | Dados recebidos pela VM |
| `Network Out Total` | Total | Bytes | Dados enviados pela VM |

### Public IP Addresses
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `PacketCount` | Total | Pacotes | Total de pacotes transmitidos |

### Storage Accounts
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `Transactions` | Total | Transações | Número de operações realizadas |

### Managed Disks
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `Composite Disk Read Operations/sec` | Average | ops/s | Operações de leitura por segundo |
| `Composite Disk Write Operations/sec` | Average | ops/s | Operações de escrita por segundo |

### Network Interfaces
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `Bytes Received` | Total | Bytes | Bytes recebidos |
| `Bytes Sent` | Total | Bytes | Bytes enviados |

### Network Security Groups
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `AllRuleHits` | Total | Hits | Total de regras acionadas |

### Virtual Networks
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `Total Inbound Packets` | Total | Pacotes | Pacotes recebidos |
| `Total Outbound Packets` | Total | Pacotes | Pacotes enviados |

### App Services / Function Apps
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `Requests` | Total | Requisições | Total de requisições |
| `CpuPercentage` | Average | % | Uso de CPU |
| `MemoryWorkingSet` | Average | Bytes | Uso de memória |

### SQL Databases
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `dtu_consumption_percent` | Average | % | Consumo de DTU |
| `cpu_percent` | Average | % | Uso de CPU |
| `storage_percent` | Average | % | Uso de armazenamento |

### Key Vaults
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `ServiceApiHit` | Total | Requisições | Total de requisições à API |
| `ServiceApiLatency` | Average | ms | Latência da API |

### Service Bus
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `IncomingMessages` | Total | Mensagens | Mensagens recebidas |
| `OutgoingMessages` | Total | Mensagens | Mensagens enviadas |

### Cosmos DB
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `TotalRequests` | Total | Requisições | Total de requisições |
| `TotalRequestUnits` | Total | RU | Consumo de unidades de requisição |
| `DataUsage` | Average | Bytes | Uso de armazenamento |

### Load Balancers
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `ByteCount` | Total | Bytes | Total de bytes processados |

### Logic Apps
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `RunsStarted` | Total | Execuções | Total de execuções iniciadas |
| `RunsFailed` | Total | Execuções | Total de execuções com falha |

### API Management
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `Requests` | Total | Requisições | Total de requisições |
| `Duration` | Average | ms | Duração média das requisições |

### AKS Clusters
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `nodesCount` | Average | Nós | Número de nós no cluster |

### Log Analytics
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `Data Ingestion` | Total | Bytes | Volume de dados ingeridos |

### Recovery Vaults
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `Backup Items` | Average | Itens | Número de itens em backup |

### Restore Point Collections
| Métrica | Agregação | Unidade | Descrição |
|---------|-----------|--------|-----------|
| `Restore Point Count` | Total | Pontos | Número de pontos de restauração |

---

## 💰 Cálculo de Custos

### Fonte dos Dados
- **API:** Azure Cost Management API
- **Endpoint:** `https://management.azure.com/subscriptions/{id}/providers/Microsoft.CostManagement/query`
- **Versão:** 2021-10-01

### Parâmetros da Consulta
| Parâmetro | Valor |
|-----------|-------|
| `type` | `ActualCost` |
| `timeframe` | `Custom` |
| `timePeriod.from` | Data de início (YYYY-MM-DD) |
| `timePeriod.to` | Data de fim (YYYY-MM-DD) |
| `granularity` | `Daily` |
| `aggregation` | `totalCost` (Sum) |

### Filtro
- **Dimensão:** `ResourceId`
- **Operador:** `In`
- **Valores:** ID do recurso específico

### Período Padrão
- **Dias:** 30 dias (configurável)
- **Cálculo diário:** `cost_30d / DAYS`

### Moeda
- **Formato:** Real Brasileiro (R$)
- **Origem:** Custo em BRL direto da API

### Tipos de Recursos com Custo
Todos os recursos que geram custo na Azure são considerados, incluindo:
- VMs (por hora de execução)
- Discos (por capacidade provisionada)
- IPs públicos (por hora de alocação)
- Storage Accounts (por capacidade e operações)
- Bancos de dados (por DTU/RU)
- Serviços gerenciados (AKS, Service Bus, etc.)

---

## 🔍 Classificação de Uso

### Metodologia de Classificação

A classificação de uso é baseada em **três pilares principais**:

1. **Custo gerado** - Recurso com custo > 0 é considerado em uso
2. **Métricas de atividade** - Tráfego, transações ou operações detectadas
3. **Dependências** - Recursos associados a outros em uso

### Níveis de Classificação

| Nível | Critério | Ação Recomendada |
|-------|----------|------------------|
| **Em Uso** | Custo > 0 OU Métricas > 0 | Manter / Monitorar |
| **Baixo Uso** | Custo > 0 mas Métricas muito baixas | Revisar redimensionamento |
| **Sem Uso** | Custo = 0 E Métricas = 0 | Candidato à remoção |
| **Órfão** | Sem dependências E Custo = 0 E Métricas = 0 | Remover |

---

## 🗑️ Critérios para Considerar "Órfão"

### Definição
Um recurso é classificado como **órfão** quando atende a **todos** os seguintes critérios:

### Critérios Obrigatórios

| # | Critério | Descrição |
|---|----------|-----------|
| 1 | **Custo = 0** | Recurso não gerou custo nos últimos 30 dias |
| 2 | **Sem dependências** | Nenhum outro recurso referencia ou depende deste |
| 3 | **Métricas zero** | Todas as métricas coletadas indicam zero atividade |

### Critérios Complementares

| Critério | Descrição |
|----------|-----------|
| **Sem tráfego** | Pacotes = 0, Bytes = 0, Transações = 0 |
| **Sem operações** | I/O operations = 0, Requisições = 0 |
| **Sem conexões** | NICs não associadas, sem endpoints |
| **Sem backups** | Pontos de restauração = 0 |

### Exemplos de Recursos Órfãos

| Recurso | Justificativa |
|---------|---------------|
| IP Público não associado e sem tráfego | Custo zero (Basic) ou custo fixo sem uso |
| Disco Premium sem VM associada | Custo por capacidade mas sem operações |
| VNet sem NICs conectadas | Recurso isolado sem tráfego |
| Backup Collection sem VM associada | Ponto de restauração órfão |
| NSG sem regras acionadas | Grupo de segurança inativo |

---

## 📁 Estrutura do Arquivo de Saída

### Nome do Arquivo
`output/azure_inventory_complete.csv`

### Formato
CSV com codificação UTF-8-BOM (compatível com Excel)

### Colunas Detalhadas

| Coluna | Tipo | Descrição | Exemplo |
|--------|------|-----------|---------|
| `id` | string | Resource ID completo | `/subscriptions/.../virtualMachines/vm1` |
| `subscriptionId` | string | ID da assinatura | `977408fa-9ef9-4bd6-bbfd-8fb6f8cc0550` |
| `resourceName` | string | Nome do recurso | `back-jor1` |
| `resourceType` | string | Tipo do recurso | `microsoft.compute/virtualmachines` |
| `resourceGroup` | string | Grupo de recursos | `GR-JOR` |
| `region` | string | Região | `brazilsouth` |
| `skuName` | string | SKU do recurso | `Standard_B4ms`, `Premium_LRS` |
| `kindName` | string | Kind (storage) | `StorageV2` |
| `managedBy` | string | Gerenciador | `aks-nodepool1` |
| `parent_resource` | string | Recurso pai | `bd01-jor` |
| `attached_resources` | string | Recursos anexados | `Disco: disk1; NIC: nic1` |
| `dependencies` | string | Dependências | `VM: back-jor1` |
| `daily_cost_30d` | float | Custo diário médio | `0.1152` |
| `cost_30d` | float | Custo total 30 dias | `3.4545` |
| `usage_summary` | string | Resumo de métricas | `CPU: 12.5%; Rede: 1223.1 GB` |
| `in_use` | string | Em uso? | `Sim` / `Não` |
| `classification` | string | Categoria | `Computação - VM` |
| `purpose` | string | Finalidade | `Virtual Machine` |
| `removal_impact` | string | Impacto da remoção | `Alto` / `Médio` / `Baixo` |
| `recommendation` | string | Ação recomendada | `Manter` / `PODE REMOVER` |
| `orphan_candidate` | string | Candidato a órfão? | `Sim` / `Não` |
| `tags` | string | Tags (JSON) | `{"Environment":"Production"}` |

---

## 📈 Exemplo de Análise e Recomendações

### Caso 1: VM Superdimensionada

**Dados:**