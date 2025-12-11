 export default {
    "connectors" : [
        {
            name : "id",
            type : "string",
            hiddenOnList:  true,
            readonly: true
        },
        {
            name : "name",
            type : "string",
            placeholder: "Please input friendly name",
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
                    value:"postgres"
                },
                {
                    name : "Star Rocks",
                    value : "starrocks"
                },
                {
                    name : "ElasticSearch",
                    value: "elasticsearch"
                },
                {
                    name : "Kafka",
                    value : "kafka"
                },
            ]
        },
        {
            name : "host",
            type : "string",
            placeholder: "127.0.0.1"
        },
        {
            name : "port",
            type : "number"
        },
        {
            name : "username",
            type : "string"
        },
        {
            name    : "password",
            type    : "string",
            hiddenOnList: true
        },
        {
            name : "database",
            type: "string",
        },
        {
            name : "extra",
            type : "json",
            hiddenOnList: true
        }
    ],
    "tasks" : [
        {
            name : "id",
            type : "string",
            readonly : true,
            hiddenOnList : true,
        },
        {
            name : "name",
            type : "string"
        },
        {
            name : "reader",
            type: "dynamic_options",
            option_type : "single",
            data_source : "connectors",
            hiddenOnList : true,
        },
        {
            name : "writers",
            type : "dynamic_options",
            option_type: "multiple",
            data_source : "connectors",
            hiddenOnList : true,
        },
        {
            name : "tables",
            type: "string",
            hiddenOnList: true,
            placeholder: "table_1,table_2,table_N"
        },
        {
            name : "extras",
            "type" : "json",
            hiddenOnList : true,
        },
        {
            name : "info",
            type: "string",
            readonly: true,
            tips: "I = Insert, U = Updated, D = Deleted"
        },
        {
            name : "current_pos",
            type: "string",
            readonly: true
        },
        {
          name : "last_synced_at",
          "type" : "string",
          readonly: true,
        },
        {
            name : "status",
            "type" : "string",
            readonly: true,
        },
        {
            name : "batch_size",
            "type" : "number",
            hiddenOnList : true,
        }
    ]
}
