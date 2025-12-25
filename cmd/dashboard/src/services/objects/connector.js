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
                    value : "es",
                }
            ]
        },
        {
            name : "name",
            type : "string",
            hiddenOnList: true,
        },
        {
            name : "host",
            type : "string"
        },
        {
            name : "port",
            type : "number"
        },
        {
            name : "username",
            type : "string",
            hiddenOnList: false,
        },
        {
            name : "password",
            type : "string",
            hiddenOnList : true
        },
        {
            name : "database",
            type : "string",
        }
    ]
}