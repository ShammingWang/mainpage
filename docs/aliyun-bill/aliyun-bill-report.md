# 产品费用分账统计项目-项目报告

# 项目背景

当前，迁云搬栈组仅有一个阿里云主账号 youshu\_testcloud\_com，由“忧叔”使用，其余成员均通过该主账号创建 RAM 子账号进行操作。按阿里云的账单体系，费用统计粒度仅支持到主账号层级，导致团队内所有成员的云资源使用成本都统一计入主账号，无法细化到个人。

这一模式使得难以追踪和核算各成员具体的云资源使用情况。例如，若某人开通了高成本服务，团队也无法准确识别并及时预警。

为提升资源使用的透明度与成本控制能力，我们希望将账单统计粒度细化至子账号（即个人）层面。这样可以更精确地识别各成员的费用占比，及时发现异常开销，并提醒相关人员关闭未使用或高成本资源，优化整体成本结构。

# 方法

目前，阿里云账单系统仅支持主账号级别的费用统计，若需实现子账号（RAM 用户）级别的分账，必须引入额外的信息。通过观察发现，对于需创建实例后才能使用的云产品，账单中通常包含实例 ID。借助这一点，我们可以利用阿里云的操作审计（ActionTrail）系统，查找每个实例的创建记录，从而获取具体的操作者（RAM 子账号）。通过将账单中的实例 ID 与审计日志中的操作记录进行关联，即可将每笔费用追溯到对应的用户，实现账单项的个体归属，从而完成子账号级别的费用分摊。

![image.png](https://alidocs.oss-cn-zhangjiakou.aliyuncs.com/res/J9LnW6jGj3KkalvD/img/f2c8e3ad-6ba0-4e79-a1f6-38a1e22aa909.png)

阿里云的账单系统

![image.png](https://alidocs.oss-cn-zhangjiakou.aliyuncs.com/res/J9LnW6jGj3KkalvD/img/29e39380-db65-4a58-87ad-e259d5fe70e8.png)

操作审计查询创建实例的操作者

# 数据源

## 操作审计

操作审计是阿里云提供的云产品，用于记录所有用户账号的操作行为。尽管阿里云提供了查询操作事件的 API 接口，但由于事件增长速度较快，使用 API 拉取数据不仅难以高效实现增量同步，还需频繁请求服务器，导致时间和资源的严重浪费。根据官方文档，操作审计支持将新增事件实时投递到 SLS（日志服务）中的 Logstore。通过启用该数据跟踪功能，我们可以将 SLS 作为稳定的数据源，持续获取全量和增量的审计事件，大幅提升数据采集的效率与可靠性。

## 阿里云账单系统

阿里云账单系统同样提供了 OpenAPI 接口，可用于获取账单数据。由于我们仅需查询当月截至当日的账单记录，即便在月末，数据量通常也不会超过 1 万条，因此可以直接将该 OpenAPI 作为数据源，每日定时调用接口，提取当日账单数据并进行全量更新。这种方式简单高效，能够满足当前账单同步的性能与数据完整性要求。

# 数据提取与清洗

## 操作审计事件数据

目前，操作审计的事件数据已实时投递至 SLS 的 Logstore 中。我们可以在 DataWorks 中配置一个以 LogHub 为数据源的同步任务，将日志数据同步至 MaxCompute 中的目标表 actiontrail。该方式实现了从日志服务到数据仓库的自动化对接，便于后续在 MaxCompute 中进行统一的查询分析与关联处理。

![image.png](https://alidocs.oss-cn-zhangjiakou.aliyuncs.com/res/J9LnW6jGj3KkalvD/img/c66f2645-32e1-4b2f-a945-5cfa290211d6.png)

从Loghub到MaxCompute的实时增量同步任务配置

目前，actiontrail 表中已持续接收来自操作审计系统的实时事件数据。由于我们的目标是将实例与具体操作者进行绑定，因此仅需保留部分关键事件，如“用户首次创建某实例”或“首次启动某实例”等操作。为此，我们需要对各类实例的创建或初次运行事件进行精确筛选。

接下来，我们可以在 DataWorks 中创建一个 MaxCompute SQL 节点，对 actiontrail 表中的原始事件进行过滤提取，将有效事件写入目标表 actiontrail\_event\_filtered，以供后续账单数据关联使用。下面我们先来看一条示例事件数据：

```json
{
  "acsRegion": "cn-hangzhou",
  "additionalEventData": {
    "CallerBid": "26842"
  },
  "apiVersion": "*",
  "eventId": "AE3B631D-B6BE-5BFC-9F14-6BAA2683B0BA",
  "eventName": "RunInstances",
  "eventRW": "Write",
  "eventSource": "ecs-cn-hangzhou-share.aliyuncs.com",
  "eventTime": "2025-07-07T01:39:09Z",
  "eventType": "ConsoleOperation",
  "eventVersion": "1",
  "recipientAccountId": "1663676968115556",
  "referencedResources": {
    "ACS::ECS::Instance": [
      "i-bp12uqx3r5o21mj0trru"
    ],
    "ACS::ECS::Image": [
      "aliyun_3_9_x64_20G_uefi_alibase_20231219.vhd"
    ]
  },
  "requestId": "AE3B631D-B6BE-5BFC-9F14-6BAA2683B0BA",
  "requestParameters": {
    "Tenancy": "default",
    "NetworkInterface.1.SecurityGroupId": "sg-bp19xclfrq0xuz3xnkob",
    "ClientPort": 17049,
    "MaxAmount": 1,
    "SystemDisk.Size": 20,
    "X-Acs-Client-Tls-Cipher-Suite": "TLS_AES_256_GCM_SHA384",
    "SourceRegionId": "cn-hangzhou",
    "NetworkType": "vpc",
    "NetworkInterface.1.NetworkInterfaceTrafficMode": "Standard",
    "MinAmount": 1,
    "ImageOptions.LoginAsNonRoot": false,
    "SystemDisk.DeleteWithInstance": true,
    "ImageId": "aliyun_3_9_x64_20G_uefi_alibase_20231219.vhd",
    "AcceptLanguage": "zh-CN",
    "ResourceNameSuffix": false,
    "SystemDisk.Category": "cloud_efficiency",
    "NetworkInterface.1.InstanceType": "Primary",
    "InstanceType": "ecs.t5-lc2m1.nano",
    "Tags": [],
    "IoOptimized": true,
    "ZoneId": "cn-hangzhou-h",
    "PrivateDnsNameOptions.HostnameType": "Custom",
    "NetworkInterface.1.VSwitchId": "vsw-bp1nrd050apxfda654dd7",
    "InstanceName": "launch-advisor-20250707",
    "InternetMaxBandwidthOut": 0,
    "HttpTokens": "optional",
    "AcsProduct": "Ecs",
    "UniqueSuffix": false,
    "Affinity": "default",
    "X-Acs-Client-Tls-Version": "TLSv1.3",
    "RegionId": "cn-hangzhou",
    "SecurityEnhancementStrategy": "Active"
  },
  "responseElements": {
    "TaskId": "",
    "RequestId": "AE3B631D-B6BE-5BFC-9F14-6BAA2683B0BA",
    "InstanceIdSets": {
      "InstanceIdSet": [
        "i-bp12uqx3r5o21mj0trru"
      ]
    }
  },
  "serviceName": "Ecs",
  "sourceIpAddress": "140.205.11.238",
  "userAgent": "ecs.console.aliyun.com",
  "userIdentity": {
    "accountId": "1663676968115556",
    "principalId": "201553751340157473",
    "sessionContext": {
      "attributes": {
        "mfaAuthenticated": "true",
        "creationDate": "2025-07-07T01:39:09Z"
      }
    },
    "type": "ram-user",
    "userName": "shami"
  }
}
```

在解析操作审计事件的 JSON 数据时，我们需重点关注以下几个核心字段：eventName、eventSource、eventTime、referencedResources 和 userIdentity。

由于不同类型的云产品在实例创建时可能共用相同的 eventName（例如均为 Create），因此需通过 eventName 与 eventSource 的组合来唯一标识具体云产品的创建事件。referencedResources 字段包含了该事件所创建或关联的实例 ID，而 userIdentity 字段记录了触发该事件的用户信息，即实际的操作者。

通过提取并过滤这类关键事件，我们即可实现对每个实例与其创建者（操作者）之间的准确绑定，为后续账单归属分析提供基础数据支撑。

```sql
-- RunInstances: 多实例拆解
-- 云服务器ECS-按量付费 + 包年包月
SELECT
    get_json_object(event, '$.eventName') AS eventName,
    instanceId,
    get_json_object(event, '$.userIdentity.userName') AS userName,
    get_json_object(event, '$.eventTime') AS eventTime
FROM
(
    SELECT
        get_json_object(event, '$.eventName') AS eventName,
        get_json_object(event, '$.userIdentity.userName') AS userName,
        regexp_extract(event, '"ACS::ECS::Instance":\s*\[(.*?)\]', 1) AS instanceIdArray,
        get_json_object(event, '$.eventTime') AS eventTime,
        event
    FROM actiontrail
    WHERE
        get_json_object(event, '$.eventName') = 'RunInstances'
        AND ds = CAST(substr('${bizmonth}', 1, 4) AS INT)
        AND substr(get_json_object(event, '$.eventTime'), 1, 7) <=
            substr('${bizmonth}', 1, 4) || '-' || substr('${bizmonth}', 5, 2)
) LATERAL VIEW EXPLODE(
    split(
        regexp_replace(
            regexp_replace(instanceIdArray, '\[|\]|\"', ''),  -- 去除 [ ] 和 "
            '\s+',                                             -- 去除空白字符
            ''
        ),
        ','
    )
) t AS instanceId
```

针对每类云产品的实例创建事件，我们可以采用统一的 SQL 模板进行事件过滤与多实例拆分。以“云服务器 ECS-按量付费”为例，通过对操作审计事件 JSON 的解析，我们可以提取事件名称（eventName）、实例 ID（instanceId）、操作者（userName）和事件时间（eventTime）等关键字段，生成目标表 actiontrail\_event\_filtered。该表仅包含上述四个字段，结构简洁清晰。对于某些事件中包含多个资源实例的情况（如批量创建），我们会将其拆解为多条记录，每条记录对应一个实例，从而确保每个实例都能准确绑定至具体操作者。接下来，我们将基于这一方式，整理出各类云产品对应的关键事件识别规则，以便后续统一处理和扩展。

| productDetail | eventName | eventSource | referencedResourceKey |
| --- | --- | --- | --- |
| 云服务器ECS-按量付费 | RunInstances | null | ACS::ECS::Instance |
| 云服务器ECS-包年包月 | RunInstances | null | ACS::ECS::Instance |
| E-MapReduce（按量计费） | RunCluster | null | null |
| 实时计算 Flink版 | CreateInstance | null | null |
| 实时数仓Hologres独享实例（按量付费） | Create | hologram.aliyuncs.com | ACS::Hologram::Instance |
| EMR StarRocks Serverless 按量 | CreateInstanceV1 | starrocks-share.cn-hangzhou.aliyuncs.com | ACS::StarRocks::Instance |
| 云原生多模数据库 Lindorm（按量付费） | Create | lindorm.aliyuncs.com | ACS::Lindorm::Instance |
| 关系型数据库 RDS（按量付费） | CreateDBInstance | rds-inc-share.aliyuncs.com | ACS::RDS::DBInstance |
| 消息队列 Kafka（Serverless版） | Create | alikafka.aliyuncs.com | ACS::AliKafka::Instance |
| 消息队列 Kafka（按量后付费） | Create | alikafka.aliyuncs.com | ACS::AliKafka::Instance |
| 阿里云 Elasticsearch（按量付费） | Create | elasticsearch.aliyuncs.com | ACS::Elasticsearch::Instance |
| 日志服务 SLS（按量付费） | CreateLogStore | %.log.aliyuncs.com | ACS::SLS::LogStore |
| 云消息队列 RabbitMQ 版（Serverless） | CreateQueue | amqp%.aliyuncs.com | null |
| 轻量消息队列（原 MNS） | CreateQueue | mns%.aliyuncs.com | ACS::MessageService::Queue |
| 大数据计算服务 MaxCompute（按量付费） | CreateProject | maxcompute%.aliyuncs.com | ACS::MaxCompute::Project |
| 容器镜像服务 ACR | Create | cr.aliyuncs.com | ACS::CR::Instance |
| PAI-DSW（按量付费） | CreateWorkspace | aiworkspace%.aliyuncs.com | ACS::PAIWorkspace::Workspace |
| Redis 开源版 - 后付费（云原生） | Create | r-kvstore%.aliyuncs.com | ACS::Redis::DBInstance |
| 传统型负载均衡 CLB（按量付费） | CreateLoadBalancer | slb%.aliyuncs.com | ACS::SLB::LoadBalancer |

## 账单数据

由于账单数据需通过 OpenAPI 接口获取，我们在 DataWorks 中创建了一个 PyODPS 3 脚本节点，通过阿里云 Python SDK 发送请求，拉取账单信息，并将数据写入 MaxCompute 中的 instance\_bill 表。

在获取的数据中，部分云产品的 instanceId 字段可能包含区域信息或其他冗余字符串，无法直接作为标准实例 ID 使用。因此，在数据写入前，我们会对 instanceId 和 tag 字段进行清洗，提取出有效信息，确保数据一致性和后续处理的准确性。清洗的规则根据如下表格进行：

| 云产品名称 | productCode 或 productDetail | 清洗规则说明 |
| --- | --- | --- |
| EMR | `product_code == 'emr'` | 取分号 `;` 前第一个字符串 |
| MaxCompute（按量付费） | `product_detail == '大数据计算服务MaxCompute（按量付费）'` | 取分号 `;` 前第一个字符串 |
| Lindorm（按量付费） | `product_code == 'hitsdb'` | 取分号 `;` 前第一个字符串 |
| SC 产品 | `product_code == 'sc'` | 取分号 `;` 后最后一个字符串 |
| RDS | `product_code == 'rds'` | 取分号 `;` 前第一个字符串 |
| 日志服务 SLS | `product_code == 'sls'` | 取分号 `;` 第 3 个字段（即索引为 2 的部分） |
| 云消息队列 RabbitMQ Serverless | `product_detail == '云消息队列RabbitMQ版Serverless系列'` | 取分号 `;` 前第一个字符串 |
| 轻量消息队列（原 MNS） | `product_detail == '轻量消息队列（原 MNS）'` | 取分号 `;` 前第一个键值对中的 value（即 `key:value` 的 value） |
| Hologres | `product_code == 'hologram'` | 取分号 `;` 前第一个字符串 |
| ECS-按量付费（Tag 字段处理） | `product_detail == '云服务器ECS-按量付费'` | 从 `tag` 字段中提取 `clusterId value:` 后的值 |
| EMR StarRocks Serverless（按量） | `product_code == 'emapreduce'` | 取分号 `;` 第二段字符串（即索引为 1 的部分） |
| PAI-DSW 按量付费 | `product_detail == 'PAI-DSW按量付费'` | 从 `tag` 字段中取冒号 `:` 后最后一个数字作为 workspace ID |
| Redis 开源版 - 后付费（云原生） | `product_detail == 'Redis 开源版 - 后付费（云原生）'` | 取分号 `;` 前第一个字符串 |

具体脚本如下所示：

```python
# -*- coding: utf-8 -*-
# 拉取阿里云账单（BssOpenApi DescribeInstanceBill）全量分页存本地



import os

os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'] = 'LTAI**************PQyf'
os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET'] = 'UD16******************XfPk'
os.environ['ALIBABA_OPENAPI_ENDPOINT'] = 'business-vpc.cn-hangzhou.aliyuncs.com'


import os
import json
from typing import List

from alibabacloud_bssopenapi20171214.client import Client as BssOpenApi20171214Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_bssopenapi20171214 import models as bss_open_api_20171214_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient

import datetime

all_items = []
date_str = None

# 当前日期作为分区
bizdate = datetime.datetime.now()
# date_str = "2025-06-30 23:59:59"
# date_str = None
if date_str is not None:
    bizdate = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

print("------------------bizdate:", bizdate)

class Sample:
    @staticmethod
    def create_client() -> BssOpenApi20171214Client:
        config = open_api_models.Config(
            access_key_id=os.environ.get('ALIBABA_CLOUD_ACCESS_KEY_ID'),
            access_key_secret=os.environ.get('ALIBABA_CLOUD_ACCESS_KEY_SECRET'),
            endpoint=os.environ.get('ALIBABA_OPENAPI_ENDPOINT')
        )
        return BssOpenApi20171214Client(config)

    @staticmethod
    def main(args: List[str] = None) -> None:
        client = Sample.create_client()
        next_token = None
        global all_items
        page = 1

        while True:
            describe_instance_bill_request = bss_open_api_20171214_models.DescribeInstanceBillRequest(
                billing_cycle=f"{bizdate.strftime('%Y-%m')}",
                is_billing_item=False,
                max_results=300,
                next_token=next_token
            )
            runtime = util_models.RuntimeOptions()

            try:
                response = client.describe_instance_bill_with_options(describe_instance_bill_request, runtime)
                body_map = response.body.to_map()
                items = body_map.get('Data', {}).get('Items', [])
                all_items.extend(items)
                print(f"Page {page} fetched: {len(items)} items, total so far: {len(all_items)}")
                next_token = body_map.get('Data', {}).get('NextToken')
                if not next_token:
                    print("All data fetched.")
                    break
                page += 1
            except Exception as error:
                print(f"Error: {str(error)}")
                try:
                    print(error.data.get("Recommend"))
                except Exception:
                    pass
                UtilClient.assert_as_string(str(error))
                break

        print(f"{len(all_items)} records saved")

# 拉取数据
Sample.main()


# 表名
table_name = 'instance_bill'

# 获取表对象
table = odps.get_table(table_name)

# 检查并删除分区确保覆盖写入
if table.exist_partition(f"ds={bizdate.strftime('%Y%m')}"):
    table.delete_partition(f"ds={bizdate.strftime('%Y%m')}")
    print(f"Deleted existing partition ds={bizdate.strftime('%Y%m')} for overwrite")

# 准备写入数据
records = []
for item in all_items:
    record = (
        item.get('AfterDiscountAmount'),
        item.get('BillAccountID'),
        item.get('BillAccountName'),
        item.get('BillingDate'),
        item.get('BillingItem'),
        item.get('BillingType'),
        item.get('BizType'),
        item.get('CommodityCode'),
        item.get('CostUnit'),
        item.get('Currency'),
        item.get('DeductedByCoupons'),
        item.get('DeductedByResourcePackage'),
        item.get('InstanceConfig'),
        item.get('InstanceID'),
        item.get('InstanceSpec'),
        item.get('InternetIP'),
        item.get('IntranetIP'),
        item.get('InvoiceDiscount'),
        item.get('Item'),
        item.get('ItemName'),
        item.get('ListPrice'),
        item.get('ListPriceUnit'),
        item.get('NickName'),
        item.get('OwnerID'),
        item.get('PipCode'),
        item.get('PretaxAmount'),
        item.get('PretaxGrossAmount'),
        item.get('ProductCode'),
        item.get('ProductDetail'),
        item.get('ProductName'),
        item.get('ProductType'),
        item.get('Region'),
        item.get('ResourceGroup'),
        item.get('ServicePeriod'),
        item.get('ServicePeriodUnit'),
        item.get('SubscriptionType'),
        item.get('Tag'),
        item.get('Usage'),
        item.get('UsageUnit'),
        item.get('Zone'),
        bizdate.strftime('%Y%m')  # 分区字段
    )

    product_code = record[27]  # ProductCode
    instance_id = record[13]   # InstanceID
    product_detail = record[28]  # ProductDetail
    tag_value = record[36]  # Tag

    # 对于 EMR 产品，取分号前第一个字符串
    if product_code == 'emr' and instance_id:
        instance_id = instance_id.split(';')[0]
        record = record[:13] + (instance_id,) + record[14:]
    
    # 对于 大数据计算服务MaxCompute（按量付费） 产品，取分号前第一个字符串
    if product_detail == '大数据计算服务MaxCompute（按量付费）' and instance_id:
        instance_id = instance_id.split(';')[0]
        record = record[:13] + (instance_id,) + record[14:]

    # 对于 云原生多模数据库Lindorm(按量付费)，取分号前第一个字符串
    if product_code == 'hitsdb' and instance_id:
        instance_id = instance_id.split(';')[0]
        record = record[:13] + (instance_id,) + record[14:]
    
    # 对于 SC 产品，取分号后最后一个字符串
    if product_code == 'sc' and instance_id:
        instance_id = instance_id.split(';')[-1]
        record = record[:13] + (instance_id,) + record[14:]
    
    # 对于 rds 产品，取分号第一个字符串
    if product_code == 'rds' and instance_id:
        instance_id = instance_id.split(';')[0]
        record = record[:13] + (instance_id,) + record[14:]

    # 对于 sls  产品，取分号第3个字符串 为logstore
    if product_code == 'sls' and instance_id:
        instance_id = instance_id.split(';')[2]
        record = record[:13] + (instance_id,) + record[14:]


    # 对于 云消息队列RabbitMQ版Serverless系列 产品，取分号第一个字符串
    if product_detail == '云消息队列RabbitMQ版Serverless系列' and instance_id:
        instance_id = instance_id.split(';')[0]
        record = record[:13] + (instance_id,) + record[14:]

    # 对于 轻量消息队列（原 MNS） 产品，取分号第一个键值对的值
    if product_detail == '轻量消息队列（原 MNS）' and instance_id:
        instance_id = instance_id.split(';')[0].split(':')[-1]
        record = record[:13] + (instance_id,) + record[14:]

    # 对于 hologres 产品，取分号前第一个字符串
    if product_code == 'hologram' and instance_id:
        instance_id = instance_id.split(';')[0]
        record = record[:13] + (instance_id,) + record[14:]

    # 对于 ECS-按量付费，根据 Tag 替换 tag 字段内容
    if product_detail == "云服务器ECS-按量付费" and tag_value:
        try:
            # 从类似 “key:acs:emr:nodeGroupType value:MASTER; key:acs:emr:clusterId value:c-9733f32047d71ed3”
            # 中提取所有 "value:xxx" 的 xxx
            if "clusterId value:" in tag_value:
                new_tag_value = tag_value.split(':')[-1]
                # print(tag_value, new_tag_value)
                record = record[:36] + (new_tag_value,) + record[37:]
        except Exception as e:
            print(f"Tag extraction failed for: {tag_value} with error: {e}")


    # 对于 EMR StarRocks Serverless 按量，取 instance_id 第二段
    if product_code == 'emapreduce' and instance_id:
        parts = instance_id.split(';')
        if len(parts) >= 2:
            instance_id = parts[1]
            record = record[:13] + (instance_id,) + record[14:]

    # 对于 PAI-DSW按量付费 产品，tag中的冒号后的最后一个数字这是工作空间的id
    if product_detail == 'PAI-DSW按量付费' and instance_id and tag_value:
        instance_id = tag_value.split(':')[-1]
        record = record[:13] + (instance_id,) + record[14:]


    # 对于 Redis 开源版 - 后付费（云原生） 产品，tag中的冒号后的最后一个数字这是工作空间的id
    if product_detail == 'Redis 开源版 - 后付费（云原生）' and instance_id:
        instance_id = instance_id.split(';')[0]
        record = record[:13] + (instance_id,) + record[14:]

    records.append(record)

# 使用正确方式写入表（使用 table.open_writer）
with table.open_writer(partition=f"ds={bizdate.strftime('%Y%m')}", create_partition=True) as writer:
    writer.write(records)

print(f"Successfully written {len(records)} records to {table_name} partition ds={bizdate.strftime('%Y%m')}.")

```

# 原始结果生成

经过前期的数据提取与清洗，我们已分别获得两张 ODS 层的原始数据表，分别记录了操作审计事件信息与账单明细信息。接下来，只需基于两张表中的 instanceId 字段进行关联，即可将实例的费用信息与其操作者信息合并，构建出一张汇总宽表 ads\_result，为后续分析与报表提供支持。关联操作可通过如下 SQL 实现：

```sql
INSERT OVERWRITE TABLE ads_result PARTITION (ds='${bizmonth}')
SELECT
    ib.AfterDiscountAmount,
    ib.BillAccountID,
    ib.BillAccountName,
    ib.BillingDate,
    ib.BillingItem,
    ib.BillingType,
    ib.BizType,
    ib.CommodityCode,
    ib.CostUnit,
    ib.Currency,
    ib.DeductedByCoupons,
    ib.DeductedByResourcePackage,
    ib.InstanceConfig,
    ib.InstanceID,
    ib.InstanceSpec,
    ib.InternetIP,
    ib.IntranetIP,
    ib.InvoiceDiscount,
    ib.Item,
    ib.ItemName,
    ib.ListPrice,
    ib.ListPriceUnit,
    ib.NickName,
    ib.OwnerID,
    ib.PipCode,
    ib.PretaxAmount,
    ib.PretaxGrossAmount,
    ib.ProductCode,
    ib.ProductDetail,
    ib.ProductName,
    ib.ProductType,
    ib.Region,
    ib.ResourceGroup,
    ib.ServicePeriod,
    ib.ServicePeriodUnit,
    ib.SubscriptionType,
    ib.Tag,
    ib.Usage,
    ib.UsageUnit,
    ib.`Zone`,
    ae.eventName,
    ae.userName AS eventUserName,
    ae.eventTime AS eventTime
FROM
    instance_bill ib
LEFT OUTER JOIN
    actiontrail_event_filtered ae
ON
    ib.InstanceID = ae.instanceId
    AND ae.ds = '${bizmonth}'
WHERE
    ib.ds = '${bizmonth}';
```

# 最终结果

在获得原始结果宽表 ads\_result 后，我们可以根据实际业务需求从中灵活展示各类数据。然而，在阿里云产品体系中，不同云产品之间存在较强的依赖关系，若强行按产品进行费用拆分，可能与用户的直观理解存在偏差。

以 E-MapReduce（按量计费） 为例，阿里云账单系统仅统计该产品本身的费用，但实际上，它运行时依赖于多台 ECS-按量付费 实例构建集群。因此，在生成最终费用明细时，我们需要先在 ads\_result 中筛选出 E-MapReduce 的实例费用，并进一步根据其集群中关联的 instanceId，匹配并合并对应的 ECS 实例费用。最终将两部分费用汇总，作为该 E-MapReduce 产品的真实使用成本。

以下为相关的 SQL 实现：

```sql
SELECT
    e.userName AS userName,
    'E-MapReduce（按量计费）' AS productDetail,
    e.emr_id AS instanceId,
    e.eventTime AS eventTime,
    COALESCE(s.emr_self_amount, 0) AS currentProductAmount,
    COALESCE(ec.ecs_amount, 0) AS relatedProductAmount,
    COALESCE(s.emr_self_amount, 0) + COALESCE(ec.ecs_amount, 0) AS totalAmount
FROM
(
    SELECT 
        DISTINCT InstanceID AS emr_id,
        EventUserName AS userName,
        EventTime AS eventTime
    FROM ads_result
    WHERE ds = '${bizmonth}'
      AND productdetail = 'E-MapReduce（按量计费）'
) e
LEFT JOIN
(
    SELECT
        InstanceID AS emr_id,
        SUM(AfterDiscountAmount) AS emr_self_amount
    FROM ads_result
    WHERE
        ds = '${bizmonth}'
        AND productdetail = 'E-MapReduce（按量计费）'
    GROUP BY
        InstanceID
) s
ON e.emr_id = s.emr_id
LEFT JOIN
(
    SELECT
        /*+ mapjoin(e) */
        e.emr_id,
        SUM(i.AfterDiscountAmount) AS ecs_amount
    FROM ads_result i
    JOIN (
        SELECT DISTINCT InstanceID AS emr_id
        FROM ads_result
        WHERE ds = '${bizmonth}'
          AND productdetail = 'E-MapReduce（按量计费）'
    ) e
    ON 1 = 1
    WHERE
        i.ds = '${bizmonth}'
        AND i.ProductCode = 'ecs'
        AND i.Tag IS NOT NULL
        AND INSTR(i.Tag, e.emr_id) > 0
    GROUP BY
        e.emr_id
) ec
ON e.emr_id = ec.emr_id
```

其他产品因为比较独立，暂时先直接按照类别分别统计费用，relatedProductAmount字段的值为0

下面给出一个样例（其他产品类似）：

```sql
SELECT
    eventusername AS userName,
    ProductDetail AS productDetail,
    InstanceID AS instanceId,
    EventTime AS eventTime,
    SUM(AfterDiscountAmount) AS currentProductAmount,
    0 AS relatedProductAmount,
    SUM(AfterDiscountAmount) AS totalAmount
FROM
    ads_result
WHERE
    ds = '${bizmonth}'
    AND ProductDetail IN (
        'flink全托管（按量付费）',
        '实时数仓Hologres独享实例（按量付费）',
        'EMR StarRocks Serverless 按量',
        '云原生多模数据库Lindorm(按量付费)',
        '关系型数据库（按量付费）',
        '关系型数据库RDS(包月)',
        '消息队列 Kafka（Serverless版）',
        '阿里云Elasticsearch',
        '日志服务',
        '云消息队列RabbitMQ版Serverless系列',
        '轻量消息队列（原 MNS）',
        '大数据计算服务MaxCompute（按量付费）',
        '容器镜像服务',
        'PAI-DSW按量付费',
        '消息队列 Kafka（按量后付费）',
        'Redis 开源版 - 后付费（云原生）',
        '传统型负载均衡 CLB（按量付费）',
        'Web应用防火墙3.0（按量付费）'
    )
GROUP BY
    eventusername,
    ProductDetail,
    InstanceID,
    EventTime
```

# 结果的查询

## 计算每个人的费用

```sql
select 
    userName,
    ROUND(SUM(currentProductAmount), 2) currentProductAmount,
    ROUND(SUM(relatedProductAmount), 2) relatedProductAmount,
    ROUND(SUM(totalAmount), 2) totalAmount
FROM final_result where ds = '${bizmonth}'
GROUP BY username
ORDER BY totalAmount DESC ;
```

## 最终结果表

```sql
select 
    userName,
    productDetail,
    instanceId,
    eventTime,
    ROUND(currentProductAmount, 2) currentProductAmount,
    ROUND(relatedProductAmount, 2)relatedProductAmount,
    ROUND(totalAmount, 2) totalAmount
FROM final_result where ds = '${bizmonth}'
ORDER BY username, productdetail, totalamount;
```