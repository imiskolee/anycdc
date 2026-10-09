export default {
    name : "connectors",
    title : "Connectors",
    description : "Manage you connectors",
    "columns" : [
        {
            name : "id",
            type: "string",
            readonly: true,
        },
        {
            name : "type",
            type : "options",
            options: [
                {
                    name : "MySQL",
                    value : "mysql"
                },
                {
                    name : "PostgresSQL",
                    value : "postgres",
                },
                {
                    name : "Starrocks",
                    value : "starrocks"
                },
                {
                    name : "ElasticSearch",
                    value : "elasticsearch",
                },
                {
                    name : "HTTP",
                    value : "http",
                }
            ]
        },
        {
            name : "name",
            type : "string",
        },
        {
            name : "host",
            type : "string",
            hideFor : ["http"]
        },
        {
            name : "port",
            type : "number",
            hideFor : ["http"]
        },
        {
            name : "username",
            type : "string",
            hiddenOnList: false,
            hideFor : ["http"]
        },
        {
            name : "password",
            type : "string",
            hiddenOnList : true,
            hideFor : ["http"]
        },
        {
            name : "database",
            type : "string",
            hideFor : ["http"]
        },
        {
            name : "extra",
            type: "json",
            placeholder : '扩展配置 (JSON)',
            placeholderFor : {
                "http" : '{\n  "url": "http://host:8080/events",\n  "batch_size": 100,\n  "max_batch_time_seconds": 60,\n  "headers": {},\n  "signing_secret": ""\n}'
            },
            hiddenOnList: true,
            tipFor : ["http"],
            tip : 'HTTP 配置格式：\n{\n  "url": "http://host:8080/events",          // 必填\n  "batch_size": 100,                        // 满 N 条发送，默认 100\n  "max_batch_time_seconds": 60,            // 最多每 N 秒发送一次，默认 60\n  "headers": { "Authorization": "Bearer xxx" }, // 可选\n  "signing_secret": ""                     // 可选，Wonder webhook 验签密钥 whsec_...\n}'
        },
    ]
}