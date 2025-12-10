<template>
  <table-page
  title="Connectors"
  description="Manage your data source connection information(s)."
  :data="data"
  :custom-renders="customRenders"
  detail-prefix="/ui/common/connectors"
  object-name="connectors"
  ></table-page>
</template>

<script setup lang="jsx">
import TablePage from "../components/TablePage.vue";
import {APISDK} from "../services/api.js";

const data = [
  {
    id: "0000-00000000-0000-000000000001",
    type: "Postgres",
    alias : "test_pg_1",
    host: "127.0.0.1",
    port : "5437",
    readers: 10,
    writers: 5,
    username : "anycdc_reader"
  },
  {
    id: "0000-00000000-0000-000000000002",
    type: "Postgres",
    alias : "test_pg_2",
    host: "127.0.0.1",
    port : "5437",
    readers: 10,
    writers: 5,
    username : "anycdc_reader"
  },
  {
    id: "0000-00000000-0000-000000000003",
    type: "MySQL",
    alias : "test_pg_1",
    host: "127.0.0.1",
    port : "3306",
    readers: 10,
    writers: 5,
    username : "anycdc_reader"
  },
]

const apiSDK = new APISDK({})

function handleDelete(record) {

  return async function() {
    await apiSDK.Delete("connectors",record.record['id'])
    window.location.reload()
  }
}

var customRenders = {
  "__action__" :  (_record)=>{
    return (
     <a href="#" onClick={handleDelete(_record)}>Delete</a>
    )
  }
}

</script>