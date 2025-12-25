export default {
    name : "tasks",
    title : "Tasks",
    description : "Manage your tasks",
    "columns" : [
        {
            name : "id",
            type: "string",
            readonly : true,
        },
        {
            name : "name",
            type : "string",
            hiddenOnList: true,
        },
        {
            name : "reader",
            type: "dynamic_options",
            option_type : "single",
            data_source : "connectors",
            hiddenOnList : true,
        },
        {
            name : "writer",
            type : "dynamic_options",
            option_type: "single",
            data_source : "connectors",
            hiddenOnList : true,
        },
        {
            name : "debug_enabled",
            type : "switch",
            hiddenOnList: true
        },
        {
            name : "dumper_enabled",
            type : "switch",
            hiddenOnList: true
        },
        {
            name : "cdc_enabled",
            type : "switch",
            hiddenOnList: true
        },
        {
            name : "migrate_enabled",
            type : "switch",
            hiddenOnList: true
        },
        {
            name : "tables",
            type: "string",
            placeholder : "table_1,table_2,table_3"
        },
        {
            name : "status",
        },
    ]
}